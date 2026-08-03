from __future__ import annotations

import shutil
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.sql import haversine_km, parquet_sources
from extractais.storage import (
    candidate_path,
    directory_size,
    ensure_space,
    evidence_root_for_partition,
    lane_for_partition,
    shared_temp_requirement,
    stop_path,
    track_path,
)


GEOMETRY_CONTRACT_VERSION = 3
GEOMETRY_EDGE_SHARDS = 64


def stop_anchor_match_path(config: AppConfig, partition: int) -> Path:
    return (
        evidence_root_for_partition(config, partition)
        / "stop_anchor_matches"
        / f"partition={partition:04d}.parquet"
    )


@contextmanager
def _report_path_growth(
    config: AppConfig,
    heartbeat_path: Path,
    phase: str,
    progress_path: Path,
    space_path: Path | None = None,
) -> Iterator[None]:
    stopped = threading.Event()

    def report() -> None:
        while True:
            try:
                progress_bytes = (
                    progress_path.stat().st_size
                    if progress_path.is_file()
                    else directory_size(progress_path)
                )
                heartbeat(
                    heartbeat_path,
                    phase,
                    progress_bytes=progress_bytes,
                    space_path=str(space_path or config.storage.temp_root),
                )
            except OSError:
                pass
            if stopped.wait(config.runtime.progress_interval_seconds):
                return

    monitor = threading.Thread(target=report, name="geometry-progress", daemon=True)
    monitor.start()
    try:
        yield
    finally:
        stopped.set()
        monitor.join()


def _write_anchor_edges(
    config: AppConfig,
    track: Path,
    anchors: Path,
    tiles: Path,
    edge_root: Path,
    spill_root: Path,
) -> None:
    distance = haversine_km(
        "t.latitude", "t.longitude", "a.latitude", "a.longitude"
    )
    connection = open_database(config, spill_root, worker=True)
    try:
        connection.execute(
            f"""
            COPY (
                WITH possible AS (
                    SELECT
                        t.mmsi,
                        t.point_seq,
                        a.nearest_port_id,
                        a.anchor_id,
                        {distance} AS anchor_distance_km
                    FROM {parquet_sources([track])} t
                    JOIN {parquet_sources([tiles])} z USING (geo_tile)
                    JOIN {parquet_sources([anchors])} a USING (anchor_id)
                    WHERE NOT t.is_kinematic_outlier
                )
                SELECT
                    *,
                    cast(hash(mmsi, point_seq) % {GEOMETRY_EDGE_SHARDS} AS INTEGER)
                        AS geometry_shard
                FROM possible
                WHERE anchor_distance_km <= {config.ports.approach_radius_km}
            )
            TO {sql_literal(str(edge_root.resolve()))}
            (
                FORMAT PARQUET,
                COMPRESSION {config.layout.compression.upper()},
                ROW_GROUP_SIZE {config.layout.row_group_size},
                PARTITION_BY (geometry_shard),
                OVERWRITE_OR_IGNORE
            )
            """
        )
    finally:
        connection.close()
        shutil.rmtree(spill_root, ignore_errors=True)


def _compact_anchor_edges(
    config: AppConfig,
    shard_path: Path,
    output: Path,
    spill_root: Path,
) -> int:
    sources = sorted(shard_path.rglob("*.parquet"))
    connection = open_database(config, spill_root, worker=True)
    try:
        return int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT
                        mmsi,
                        point_seq,
                        nearest_port_id,
                        arg_min(
                            anchor_id,
                            struct_pack(
                                distance := anchor_distance_km,
                                anchor := anchor_id
                            )
                        ) AS nearest_anchor_id,
                        min(anchor_distance_km) AS anchor_distance_km
                    FROM {parquet_sources(sources)}
                    GROUP BY mmsi, point_seq, nearest_port_id
                    """,
                    output,
                    config,
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(spill_root, ignore_errors=True)


def _write_empty_point_candidates(
    config: AppConfig, output: Path, spill_root: Path
) -> int:
    connection = open_database(config, spill_root, worker=True)
    try:
        return int(
            connection.execute(
                parquet_copy_sql(
                    """
                    SELECT
                        NULL::BIGINT AS mmsi,
                        NULL::BIGINT AS point_seq,
                        NULL::TIMESTAMP AS timestamp_utc,
                        NULL::INTEGER AS track_partition_id,
                        NULL::VARCHAR AS nearest_port_id,
                        NULL::VARCHAR AS nearest_anchor_id,
                        NULL::DOUBLE AS anchor_distance_km
                    WHERE false
                    """,
                    output,
                    config,
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(spill_root, ignore_errors=True)


def _write_stop_matches(
    config: AppConfig,
    stops: Path,
    anchors: Path,
    output: Path,
    spill_root: Path,
) -> int:
    stop_distance = haversine_km(
        "s.centroid_latitude", "s.centroid_longitude", "a.latitude", "a.longitude"
    )
    connection = open_database(config, spill_root, worker=True)
    try:
        return int(
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
                    output,
                    config,
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(spill_root, ignore_errors=True)


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
    temporary_outputs = tuple(
        path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs
    )
    worker_root = config.storage.temp_root / f"geometry-{partition:04d}"
    edge_root = worker_root / "point-anchor-edges"
    compact_root = worker_root / "point-port-candidates"
    shutil.rmtree(worker_root, ignore_errors=True)
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

    try:
        phase = "1/4 extracting point-anchor edge shards"
        with _report_path_growth(config, heartbeat_path, phase, edge_root):
            _write_anchor_edges(
                config,
                track,
                anchors,
                tiles,
                edge_root,
                worker_root / "spill-edges",
            )

        shard_paths = sorted(
            (path for path in edge_root.glob("geometry_shard=*") if path.is_dir()),
            key=lambda path: int(path.name.rsplit("=", 1)[1]),
        )
        compact_paths: list[Path] = []
        candidate_count = 0
        for index, shard_path in enumerate(shard_paths, start=1):
            shard = int(shard_path.name.rsplit("=", 1)[1])
            compact_output = compact_root / f"part-{shard:02d}.parquet"
            compact_output.parent.mkdir(parents=True, exist_ok=True)
            phase = f"2/4 compacting point-port shard {index}/{len(shard_paths)}"
            with _report_path_growth(
                config, heartbeat_path, phase, compact_output
            ):
                candidate_count += _compact_anchor_edges(
                    config,
                    shard_path,
                    compact_output,
                    worker_root / f"spill-compact-{shard:02d}",
                )
            compact_paths.append(compact_output)
            shutil.rmtree(shard_path, ignore_errors=True)

        compact_bytes = sum(path.stat().st_size for path in compact_paths)
        ensure_space(
            evidence_root,
            config.storage.reserves_gib["evidence"],
            track.stat().st_size + compact_bytes * 2,
            f"compacted geometry evidence {partition:04d}",
        )
        if compact_paths:
            connection = open_database(
                config, worker_root / "spill-merge", worker=True
            )
            try:
                with _report_path_growth(
                    config,
                    heartbeat_path,
                    "3/4 merging compact point-port candidates",
                    temporary_outputs[0],
                    evidence_root,
                ):
                    merged_count = int(
                        connection.execute(
                            parquet_copy_sql(
                                f"""
                                SELECT
                                    c.mmsi,
                                    c.point_seq,
                                    t.timestamp_utc,
                                    t.track_partition_id,
                                    c.nearest_port_id,
                                    c.nearest_anchor_id,
                                    c.anchor_distance_km
                                FROM {parquet_sources(compact_paths)} c
                                JOIN {parquet_sources([track])} t USING (mmsi, point_seq)
                                """,
                                temporary_outputs[0],
                                config,
                            )
                        ).fetchone()[0]
                    )
            finally:
                connection.close()
                shutil.rmtree(worker_root / "spill-merge", ignore_errors=True)
            if merged_count != candidate_count:
                raise RuntimeError(
                    f"Geometry candidate count changed during merge: "
                    f"{candidate_count} != {merged_count}"
                )
        else:
            candidate_count = _write_empty_point_candidates(
                config, temporary_outputs[0], worker_root / "spill-empty"
            )

        with _report_path_growth(
            config,
            heartbeat_path,
            "4/4 matching stop events to anchors",
            temporary_outputs[1],
            evidence_root,
        ):
            stop_count = _write_stop_matches(
                config,
                stops,
                anchors,
                temporary_outputs[1],
                worker_root / "spill-stops",
            )

        heartbeat(heartbeat_path, "committing")
        for temporary, output in zip(temporary_outputs, outputs):
            atomic_replace(temporary, output)
    except BaseException:
        for temporary in temporary_outputs:
            temporary.unlink(missing_ok=True)
        raise
    finally:
        shutil.rmtree(worker_root, ignore_errors=True)

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
            GEOMETRY_CONTRACT_VERSION,
            GEOMETRY_EDGE_SHARDS,
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
        beat = (
            config.storage.temp_root
            / "heartbeats"
            / f"geometry-{partition:04d}.json"
        )
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
