from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.sql import haversine_km, parquet_sources
from extractais.storage import (
    candidate_path,
    ensure_space,
    evidence_root_for_partition,
    lane_for_partition,
    shared_temp_requirement,
    stop_path,
    track_path,
)


def stop_anchor_match_path(config: AppConfig, partition: int) -> Path:
    return evidence_root_for_partition(config, partition) / "stop_anchor_matches" / f"partition={partition:04d}.parquet"


def _geometry_worker(
    config: AppConfig,
    partition: int,
    outputs: tuple[Path, Path],
    heartbeat_path: Path,
) -> dict[str, int]:
    track = track_path(config, partition)
    stops = stop_path(config, partition)
    ports_root = config.storage.products_root / "ports"
    anchors = ports_root / "anchors.parquet"
    tiles = ports_root / "anchor_tiles.parquet"
    temporary_outputs = tuple(path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs)
    worker_temp = config.storage.temp_root / f"geometry-{partition:04d}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)

    evidence_root = evidence_root_for_partition(config, partition)
    ensure_space(
        evidence_root,
        config.storage.reserves_gib["evidence"],
        track.stat().st_size * 2,
        f"geometry evidence {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"geometry temporary {partition:04d}",
    )

    connection = open_database(config, worker_temp, worker=True)
    try:
        heartbeat(heartbeat_path, "matching AIS points to anchors")
        point_distance = haversine_km("t.latitude", "t.longitude", "a.latitude", "a.longitude")
        candidate_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    WITH possible AS (
                        SELECT
                            t.mmsi,
                            t.point_seq,
                            t.timestamp_utc,
                            t.latitude,
                            t.longitude,
                            t.speed,
                            t.source_at_dock,
                            t.gap_seconds,
                            t.matched_port_name,
                            t.source_label,
                            t.source_sublabel,
                            t.collection_type,
                            t.source,
                            t.track_partition_id,
                            a.anchor_id,
                            {point_distance} AS anchor_distance_km
                        FROM {parquet_sources([track])} t
                        JOIN {parquet_sources([tiles])} z USING (geo_tile)
                        JOIN {parquet_sources([anchors])} a USING (anchor_id)
                        WHERE NOT t.is_kinematic_outlier
                    )
                    SELECT *
                    FROM possible
                    WHERE anchor_distance_km <= {config.ports.approach_radius_km}
                    """,
                    temporary_outputs[0],
                    config,
                    order_by="mmsi, point_seq, anchor_distance_km, anchor_id",
                )
            ).fetchone()[0]
        )

        heartbeat(heartbeat_path, "matching stop events to anchors")
        stop_distance = haversine_km(
            "s.centroid_latitude", "s.centroid_longitude", "a.latitude", "a.longitude"
        )
        stop_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    WITH possible AS (
                        SELECT
                            s.stop_id,
                            s.mmsi,
                            s.start_time_utc,
                            s.end_time_utc,
                            s.track_partition_id,
                            a.anchor_id,
                            {stop_distance} AS anchor_distance_km
                        FROM {parquet_sources([stops])} s
                        JOIN {parquet_sources([anchors])} a
                          ON abs(s.centroid_latitude - a.latitude)
                                <= {config.ports.entry_radius_km / 111.0 + 0.01}
                         AND abs(((s.centroid_longitude - a.longitude + 540.0) % 360.0) - 180.0)
                                <= 0.25
                    ),
                    ranked AS (
                        SELECT *, row_number() OVER (
                            PARTITION BY stop_id ORDER BY anchor_distance_km, anchor_id
                        ) AS match_rank
                        FROM possible
                        WHERE anchor_distance_km <= {config.ports.entry_radius_km}
                    )
                    SELECT * EXCLUDE (match_rank)
                    FROM ranked
                    WHERE match_rank = 1
                    """,
                    temporary_outputs[1],
                    config,
                    order_by="mmsi, start_time_utc, stop_id",
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)

    heartbeat(heartbeat_path, "committing")
    for temporary, output in zip(temporary_outputs, outputs):
        atomic_replace(temporary, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": candidate_count + stop_count,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def build_geometry(config: AppConfig, store: CheckpointStore) -> None:
    ports_root = config.storage.products_root / "ports"
    anchors = ports_root / "anchors.parquet"
    tiles = ports_root / "anchor_tiles.parquet"
    geometry_hash = signature(
        [
            config.ports.approach_radius_km,
            config.ports.entry_radius_km,
            anchors.stat().st_size,
            anchors.stat().st_mtime_ns,
            tiles.stat().st_size,
            tiles.stat().st_mtime_ns,
        ]
    )
    tasks: list[StageTask] = []
    for partition in range(config.layout.track_partitions):
        track = track_path(config, partition)
        stops = stop_path(config, partition)
        outputs = (
            candidate_path(config, partition),
            stop_anchor_match_path(config, partition),
        )
        task_signature = signature(
            [
                geometry_hash,
                track.stat().st_size,
                track.stat().st_mtime_ns,
                stops.stat().st_size,
                stops.stat().st_mtime_ns,
            ]
        )
        beat = config.storage.temp_root / "heartbeats" / f"geometry-{partition:04d}.json"
        tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=task_signature,
                source_bytes=max(1, track.stat().st_size + stops.stat().st_size),
                outputs=outputs,
                heartbeat_path=beat,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_geometry_worker,
                    args=(config, partition, outputs, beat),
                    resource=(
                        f"{lane_for_partition(config, partition)}|"
                        f"{evidence_root_for_partition(config, partition)}"
                    ),
                ),
            )
        )

    run_stage_tasks(
        config,
        store,
        "geometry",
        tasks,
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )
