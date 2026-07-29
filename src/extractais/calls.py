from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.geometry import stop_anchor_match_path
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.sql import parquet_sources
from extractais.storage import (
    candidate_path,
    ensure_space,
    evidence_root_for_partition,
    lane_for_partition,
    port_call_path,
    port_context_path,
    shared_output_requirement,
    shared_temp_requirement,
    track_path,
)


def group_candidate_path(config: AppConfig, partition: int) -> Path:
    return evidence_root_for_partition(config, partition) / "point_group_candidates" / f"partition={partition:04d}.parquet"


def _temporary(path: Path) -> Path:
    return path.with_name(path.stem + ".tmp" + path.suffix)


def _calls_worker(
    config: AppConfig,
    partition: int,
    outputs: tuple[Path, Path, Path],
    heartbeat_path: Path,
) -> dict[str, int]:
    point_candidates = candidate_path(config, partition)
    stop_matches = stop_anchor_match_path(config, partition)
    ports_root = config.storage.products_root / "ports"
    anchors = ports_root / "anchors.parquet"
    catalog = ports_root / "port_catalog.parquet"
    groups = ports_root / "port_groups.parquet"
    temporary_outputs = tuple(_temporary(path) for path in outputs)
    worker_temp = config.storage.temp_root / f"calls-{partition:04d}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    ensure_space(
        evidence_root_for_partition(config, partition),
        config.storage.reserves_gib["evidence"],
        shared_output_requirement(config, point_candidates.stat().st_size * 2),
        f"group candidate evidence {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"calls temporary {partition:04d}",
    )
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        shared_output_requirement(config, max(stop_matches.stat().st_size, 1024**2)),
        f"port calls {partition:04d}",
    )

    connection = open_database(config, worker_temp, worker=True)
    try:
        heartbeat(heartbeat_path, "mapping anchor evidence to port groups")
        connection.execute(
            f"""
            CREATE TEMP TABLE group_candidates AS
            WITH mapped AS (
                SELECT
                    c.*,
                    p.port_group_id,
                    p.port_group_name,
                    g.country_or_area_name AS port_country_or_area,
                    a.nearest_port_id
                FROM {parquet_sources([point_candidates])} c
                JOIN {parquet_sources([anchors])} a USING (anchor_id)
                JOIN {parquet_sources([catalog])} p
                  ON a.nearest_port_id = p.port_id
                JOIN {parquet_sources([groups])} g USING (port_group_id)
            ),
            by_group AS (
                SELECT
                    mmsi, point_seq, timestamp_utc, latitude, longitude, speed,
                    source_at_dock, gap_seconds, matched_port_name,
                    source_label, source_sublabel, collection_type, source,
                    track_partition_id, port_group_id,
                    arg_min(port_group_name, anchor_distance_km) AS port_group_name,
                    arg_min(port_country_or_area, anchor_distance_km)
                        AS port_country_or_area,
                    arg_min(anchor_id, anchor_distance_km) AS nearest_anchor_id,
                    arg_min(nearest_port_id, anchor_distance_km) AS nearest_port_id,
                    min(anchor_distance_km) AS port_distance_km
                FROM mapped
                GROUP BY
                    mmsi, point_seq, timestamp_utc, latitude, longitude, speed,
                    source_at_dock, gap_seconds, matched_port_name,
                    source_label, source_sublabel, collection_type, source,
                    track_partition_id, port_group_id
            )
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY mmsi, point_seq
                    ORDER BY port_distance_km, port_group_id
                ) AS candidate_rank
            FROM by_group
            """
        )
        group_count = int(
            connection.execute(
                parquet_copy_sql(
                    "SELECT * FROM group_candidates",
                    temporary_outputs[0],
                    config,
                    order_by="mmsi, point_seq, candidate_rank",
                )
            ).fetchone()[0]
        )

        heartbeat(heartbeat_path, "building compact point context")
        connection.execute(
            """
            CREATE TEMP TABLE port_context AS
            SELECT
                mmsi,
                point_seq,
                arg_min(timestamp_utc, candidate_rank) AS timestamp_utc,
                arg_min(source_at_dock, candidate_rank) AS source_at_dock,
                arg_min(matched_port_name, candidate_rank) AS matched_port_name,
                arg_min(track_partition_id, candidate_rank) AS track_partition_id,
                arg_min(port_group_id, candidate_rank) AS port_group_id,
                arg_min(port_group_name, candidate_rank) AS port_group_name,
                arg_min(port_country_or_area, candidate_rank) AS port_country_or_area,
                arg_min(nearest_anchor_id, candidate_rank) AS nearest_anchor_id,
                max(port_group_id) FILTER (WHERE candidate_rank = 2)
                    AS second_port_group_id,
                max(port_group_name) FILTER (WHERE candidate_rank = 2)
                    AS second_port_group_name,
                max(port_country_or_area) FILTER (WHERE candidate_rank = 2)
                    AS second_port_country_or_area,
                min(port_distance_km) FILTER (WHERE candidate_rank = 1)
                    AS port_distance_km,
                min(port_distance_km) FILTER (WHERE candidate_rank = 2)
                    AS second_port_distance_km,
                min(port_distance_km) FILTER (WHERE candidate_rank = 2)
                  - min(port_distance_km) FILTER (WHERE candidate_rank = 1)
                    AS ambiguity_margin_km
            FROM group_candidates
            WHERE candidate_rank <= 2
            GROUP BY mmsi, point_seq
            """
        )
        context_count = int(
            connection.execute(
                parquet_copy_sql(
                    "SELECT * FROM port_context",
                    temporary_outputs[1],
                    config,
                    order_by="mmsi, point_seq",
                )
            ).fetchone()[0]
        )

        heartbeat(heartbeat_path, "confirming port calls")
        connection.execute(
            f"""
            CREATE TEMP TABLE stop_group_matches AS
            SELECT
                s.stop_id, s.mmsi, s.start_time_utc, s.end_time_utc,
                p.port_group_id
            FROM {parquet_sources([stop_matches])} s
            JOIN {parquet_sources([anchors])} a USING (anchor_id)
            JOIN {parquet_sources([catalog])} p
              ON a.nearest_port_id = p.port_id
            """
        )
        maximum_gap_seconds = int(config.ports.call_max_point_gap_hours * 3600)
        call_sql = f"""
            WITH previous AS (
                SELECT
                    *,
                    lag(point_seq) OVER vessel_order AS previous_point_seq,
                    lag(port_group_id) OVER vessel_order AS previous_port_group_id,
                    lag(timestamp_utc) OVER vessel_order AS previous_time_utc
                FROM port_context
                WINDOW vessel_order AS (PARTITION BY mmsi ORDER BY point_seq)
            ),
            marked AS (
                SELECT
                    *,
                    CASE WHEN previous_point_seq IS NULL
                           OR point_seq <> previous_point_seq + 1
                           OR port_group_id <> previous_port_group_id
                           OR date_diff('second', previous_time_utc, timestamp_utc)
                                > {maximum_gap_seconds}
                         THEN 1 ELSE 0 END AS new_episode
                FROM previous
            ),
            grouped AS (
                SELECT
                    *,
                    sum(new_episode) OVER (
                        PARTITION BY mmsi ORDER BY point_seq ROWS UNBOUNDED PRECEDING
                    ) AS episode_number
                FROM marked
            ),
            episodes AS (
                SELECT
                    mmsi, track_partition_id, episode_number, port_group_id,
                    arg_min(port_group_name, timestamp_utc) AS port_group_name,
                    arg_min(port_country_or_area, timestamp_utc)
                        AS port_country_or_area,
                    min(timestamp_utc) AS approach_start_time_utc,
                    max(timestamp_utc) AS approach_end_time_utc,
                    min(timestamp_utc) FILTER (
                        WHERE port_distance_km <= {config.ports.entry_radius_km}
                    ) AS entry_time_utc,
                    max(timestamp_utc) FILTER (
                        WHERE port_distance_km <= {config.ports.exit_radius_km}
                    ) AS exit_time_utc,
                    count(*) AS point_count,
                    min(port_distance_km) AS minimum_port_distance_km,
                    count(*) FILTER (WHERE source_at_dock) AS source_at_dock_points,
                    count(*) FILTER (WHERE matched_port_name IS NOT NULL)
                        AS source_matched_port_points,
                    min(ambiguity_margin_km) AS minimum_ambiguity_margin_km
                FROM grouped
                GROUP BY mmsi, track_partition_id, episode_number, port_group_id
            ),
            evidence AS (
                SELECT
                    e.*,
                    count(DISTINCT s.stop_id) AS matched_stop_count
                FROM episodes e
                LEFT JOIN stop_group_matches s
                  ON e.mmsi = s.mmsi
                 AND e.port_group_id = s.port_group_id
                 AND s.end_time_utc >= e.approach_start_time_utc
                 AND s.start_time_utc <= e.approach_end_time_utc
                GROUP BY ALL
            )
            SELECT
                md5(concat(mmsi::VARCHAR, '|', port_group_id, '|', entry_time_utc::VARCHAR))
                    AS port_call_id,
                *,
                minimum_ambiguity_margin_km < {config.ports.ambiguity_margin_km}
                    AS has_port_ambiguity
            FROM evidence
            WHERE entry_time_utc IS NOT NULL
              AND exit_time_utc IS NOT NULL
              AND point_count >= {config.ports.call_min_points}
              AND matched_stop_count > 0
        """
        call_count = int(
            connection.execute(
                parquet_copy_sql(
                    call_sql,
                    temporary_outputs[2],
                    config,
                    order_by="mmsi, entry_time_utc, port_call_id",
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
        "row_count": call_count,
        "candidate_count": group_count,
        "context_count": context_count,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def build_calls(config: AppConfig, store: CheckpointStore) -> None:
    ports_root = config.storage.products_root / "ports"
    dependencies = [
        ports_root / "anchors.parquet",
        ports_root / "port_catalog.parquet",
        ports_root / "port_groups.parquet",
    ]
    global_signature = signature(
        [
            config.raw["ports"],
            [(path.stat().st_size, path.stat().st_mtime_ns) for path in dependencies],
        ]
    )
    tasks: list[StageTask] = []
    for partition in range(config.layout.track_partitions):
        candidates = candidate_path(config, partition)
        stop_matches = stop_anchor_match_path(config, partition)
        outputs = (
            group_candidate_path(config, partition),
            port_context_path(config, partition),
            port_call_path(config, partition),
        )
        task_signature = signature(
            [
                global_signature,
                candidates.stat().st_size,
                candidates.stat().st_mtime_ns,
                stop_matches.stat().st_size,
                stop_matches.stat().st_mtime_ns,
            ]
        )
        beat = config.storage.temp_root / "heartbeats" / f"calls-{partition:04d}.json"
        tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=task_signature,
                source_bytes=max(1, candidates.stat().st_size + stop_matches.stat().st_size),
                outputs=outputs,
                heartbeat_path=beat,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_calls_worker,
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
        "calls",
        tasks,
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )
