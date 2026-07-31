from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

import duckdb

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.ingest import dynamic_run_path, static_run_path, stats_run_path
from extractais.inventory import Inventory
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.sql import haversine_km, parquet_sources
from extractais.storage import (
    ensure_space,
    lane_for_partition,
    shared_output_requirement,
    shared_temp_requirement,
    stop_path,
    total_file_size,
    track_path,
)


def vessel_path(config: AppConfig, partition: int) -> Path:
    return config.storage.products_root / "vessels" / f"partition={partition:04d}.parquet"


def _temporary(path: Path) -> Path:
    return path.with_name(path.stem + ".tmp" + path.suffix)


def _reference_canonical_sql(
    config: AppConfig,
    dynamic_paths: list[Path],
    partition: int,
) -> str:
    """Preserve the v2.0.1 query as the staged-output regression oracle."""
    source = parquet_sources(dynamic_paths)
    distance = haversine_km(
        "previous_latitude", "previous_longitude", "latitude", "longitude"
    )
    return f"""
        WITH selected AS (
            SELECT *
            FROM {source}
            WHERE track_partition_id = {partition}
        ),
        exact_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY mmsi, timestamp_utc, latitude, longitude
                    ORDER BY source_at_dock DESC NULLS LAST, msg_type, msg_id NULLS LAST
                ) AS exact_rank
            FROM selected
        ),
        deduplicated AS (
            SELECT * EXCLUDE (exact_rank)
            FROM exact_ranked
            WHERE exact_rank = 1
        ),
        conflicts AS (
            SELECT
                *,
                count(*) OVER (PARTITION BY mmsi, timestamp_utc) > 1
                    AS is_time_conflict
            FROM deduplicated
        ),
        sequenced AS (
            SELECT
                *,
                row_number() OVER vessel_order AS point_seq,
                lag(timestamp_utc) OVER vessel_order AS previous_timestamp_utc,
                lag(latitude) OVER vessel_order AS previous_latitude,
                lag(longitude) OVER vessel_order AS previous_longitude
            FROM conflicts
            WINDOW vessel_order AS (
                PARTITION BY mmsi ORDER BY timestamp_utc, latitude, longitude
            )
        ),
        measured AS (
            SELECT
                *,
                date_diff('second', previous_timestamp_utc, timestamp_utc)
                    AS gap_seconds,
                CASE WHEN previous_timestamp_utc IS NOT NULL THEN {distance} END
                    AS step_distance_km
            FROM sequenced
        ),
        speeds AS (
            SELECT
                *,
                CASE WHEN gap_seconds > 0
                     THEN step_distance_km / gap_seconds * 3600.0 / 1.852 END
                    AS implied_speed_knots
            FROM measured
        )
        SELECT
            * EXCLUDE (
                previous_timestamp_utc, previous_latitude, previous_longitude
            ),
            coalesce(
                implied_speed_knots > {config.cleaning.max_implied_speed_knots},
                false
            ) AS is_kinematic_outlier
        FROM speeds
    """


def _deduplicated_sql(dynamic_paths: list[Path], partition: int) -> str:
    source = parquet_sources(dynamic_paths)
    return f"""
        WITH selected AS (
            SELECT *
            FROM {source}
            WHERE track_partition_id = {partition}
        ),
        exact_ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY mmsi, timestamp_utc, latitude, longitude
                    ORDER BY source_at_dock DESC NULLS LAST, msg_type, msg_id NULLS LAST
                ) AS exact_rank
            FROM selected
        )
        SELECT * EXCLUDE (exact_rank)
        FROM exact_ranked
        WHERE exact_rank = 1
    """


def _conflict_sql(deduplicated_path: Path) -> str:
    source = parquet_sources([deduplicated_path])
    return f"""
        SELECT mmsi, timestamp_utc
        FROM {source}
        GROUP BY mmsi, timestamp_utc
        HAVING count(*) > 1
    """


def _sequenced_sql(
    config: AppConfig,
    deduplicated_path: Path,
    conflict_path: Path,
) -> str:
    deduplicated = parquet_sources([deduplicated_path])
    conflicts = parquet_sources([conflict_path])
    distance = haversine_km(
        "previous_latitude", "previous_longitude", "latitude", "longitude"
    )
    return f"""
        WITH conflict_marked AS (
            SELECT
                points.*,
                conflict_times.mmsi IS NOT NULL AS is_time_conflict
            FROM {deduplicated} AS points
            LEFT JOIN {conflicts} AS conflict_times
                USING (mmsi, timestamp_utc)
        ),
        sequenced AS (
            SELECT
                *,
                row_number() OVER vessel_order AS point_seq,
                lag(timestamp_utc) OVER vessel_order AS previous_timestamp_utc,
                lag(latitude) OVER vessel_order AS previous_latitude,
                lag(longitude) OVER vessel_order AS previous_longitude
            FROM conflict_marked
            WINDOW vessel_order AS (
                PARTITION BY mmsi ORDER BY timestamp_utc, latitude, longitude
            )
        ),
        measured AS (
            SELECT
                *,
                date_diff('second', previous_timestamp_utc, timestamp_utc)
                    AS gap_seconds,
                CASE WHEN previous_timestamp_utc IS NOT NULL THEN {distance} END
                    AS step_distance_km
            FROM sequenced
        ),
        speeds AS (
            SELECT
                *,
                CASE WHEN gap_seconds > 0
                     THEN step_distance_km / gap_seconds * 3600.0 / 1.852 END
                    AS implied_speed_knots
            FROM measured
        )
        SELECT
            * EXCLUDE (
                previous_timestamp_utc, previous_latitude, previous_longitude
            ),
            coalesce(
                implied_speed_knots > {config.cleaning.max_implied_speed_knots},
                false
            ) AS is_kinematic_outlier
        FROM speeds
    """


def _stop_sql(
    config: AppConfig,
    partition: int,
    track_source: str = "canonical_tracks",
) -> str:
    diameter = haversine_km(
        "min_latitude", "min_longitude", "max_latitude", "max_longitude"
    )
    return f"""
        WITH candidates AS (
            SELECT
                *,
                (
                    coalesce(speed <= {config.stops.max_speed_knots}, false)
                    OR (
                        speed IS NULL
                        AND coalesce(step_distance_km <= {config.stops.max_step_km}, false)
                    )
                ) AS is_stop_candidate
            FROM {track_source}
            WHERE NOT is_kinematic_outlier
        ),
        previous AS (
            SELECT
                *,
                lag(is_stop_candidate, 1, false) OVER (
                    PARTITION BY mmsi ORDER BY point_seq
                ) AS previous_is_stop_candidate
            FROM candidates
        ),
        marked AS (
            SELECT
                *,
                CASE WHEN is_stop_candidate AND (
                    NOT previous_is_stop_candidate
                    OR gap_seconds IS NULL
                    OR gap_seconds > {config.stops.max_point_gap_minutes * 60}
                ) THEN 1 ELSE 0 END AS new_stop_run
            FROM previous
        ),
        grouped AS (
            SELECT
                *,
                sum(new_stop_run) OVER (
                    PARTITION BY mmsi ORDER BY point_seq ROWS UNBOUNDED PRECEDING
                ) AS stop_run
            FROM marked
        ),
        aggregated AS (
            SELECT
                mmsi,
                stop_run,
                min(timestamp_utc) AS start_time_utc,
                max(timestamp_utc) AS end_time_utc,
                date_diff('second', min(timestamp_utc), max(timestamp_utc))
                    AS duration_seconds,
                count(*) AS point_count,
                avg(latitude) AS centroid_latitude,
                avg(longitude) AS centroid_longitude,
                min(latitude) AS min_latitude,
                max(latitude) AS max_latitude,
                min(longitude) AS min_longitude,
                max(longitude) AS max_longitude,
                min(speed) AS min_speed_knots,
                max(speed) AS max_speed_knots,
                avg(speed) AS mean_speed_knots,
                count(*) FILTER (WHERE source_at_dock) AS source_at_dock_points,
                max(gap_seconds) AS max_gap_seconds
            FROM grouped
            WHERE is_stop_candidate
            GROUP BY mmsi, stop_run
        ),
        measured AS (
            SELECT *, {diameter} AS diameter_km
            FROM aggregated
        )
        SELECT
            md5(concat(mmsi::VARCHAR, '|', start_time_utc::VARCHAR, '|', end_time_utc::VARCHAR))
                AS stop_id,
            *,
            {partition}::INTEGER AS track_partition_id
        FROM measured
        WHERE duration_seconds >= {config.stops.min_duration_minutes * 60}
          AND diameter_km <= {config.stops.max_diameter_km}
    """


def _copy_phase(
    config: AppConfig,
    worker_temp: Path,
    heartbeat_path: Path,
    *,
    phase_index: int,
    phase_total: int,
    phase_key: str,
    label: str,
    select_sql: str,
    output_path: Path,
    order_by: str | None,
    space_path: Path,
) -> int:
    spill = worker_temp / f"spill-{phase_key}"
    shutil.rmtree(spill, ignore_errors=True)
    output_path.unlink(missing_ok=True)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    heartbeat(
        heartbeat_path,
        f"{phase_index}/{phase_total} {label}",
        progress_path=str(output_path.resolve()),
        space_path=str(space_path.resolve()),
    )
    connection = open_database(config, spill, worker=True)
    try:
        return int(
            connection.execute(
                parquet_copy_sql(
                    select_sql,
                    output_path,
                    config,
                    order_by=order_by,
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(spill, ignore_errors=True)


def _vessel_sql(static_paths: list[Path], partition: int) -> str:
    source = parquet_sources(static_paths)
    return f"""
        SELECT
            mmsi,
            min(timestamp_utc) AS first_static_time_utc,
            max(timestamp_utc) AS last_static_time_utc,
            count(*) AS static_message_count,
            arg_max(imo, timestamp_utc) FILTER (WHERE imo IS NOT NULL) AS imo,
            arg_max(flag, timestamp_utc) FILTER (WHERE flag IS NOT NULL) AS flag,
            arg_max(draught, timestamp_utc) FILTER (WHERE draught IS NOT NULL) AS draught,
            arg_max(ship_and_cargo_type, timestamp_utc)
                FILTER (WHERE ship_and_cargo_type IS NOT NULL) AS ship_and_cargo_type,
            arg_max(length, timestamp_utc) FILTER (WHERE length IS NOT NULL) AS length,
            arg_max(width, timestamp_utc) FILTER (WHERE width IS NOT NULL) AS width,
            arg_max(eta_raw, timestamp_utc) FILTER (WHERE eta_raw IS NOT NULL) AS eta_raw,
            arg_max(to_bow, timestamp_utc) FILTER (WHERE to_bow IS NOT NULL) AS to_bow,
            arg_max(to_stern, timestamp_utc) FILTER (WHERE to_stern IS NOT NULL) AS to_stern,
            arg_max(to_port, timestamp_utc) FILTER (WHERE to_port IS NOT NULL) AS to_port,
            arg_max(to_starboard, timestamp_utc)
                FILTER (WHERE to_starboard IS NOT NULL) AS to_starboard,
            arg_max(ais_version, timestamp_utc)
                FILTER (WHERE ais_version IS NOT NULL) AS ais_version,
            arg_max(ship_type, timestamp_utc) FILTER (WHERE ship_type IS NOT NULL) AS ship_type,
            arg_max(collection_type, timestamp_utc)
                FILTER (WHERE collection_type IS NOT NULL) AS collection_type,
            arg_max(source, timestamp_utc) FILTER (WHERE source IS NOT NULL) AS source,
            {partition}::INTEGER AS track_partition_id
        FROM {source}
        WHERE track_partition_id = {partition}
        GROUP BY mmsi
    """


def _track_worker(
    config: AppConfig,
    partition: int,
    dynamic_paths: list[Path],
    static_paths: list[Path],
    outputs: tuple[Path, Path, Path],
    heartbeat_path: Path,
    estimated_source_bytes: int | None = None,
) -> dict[str, int]:
    track_output, stop_output, vessel_output = outputs
    temporary_outputs = tuple(_temporary(path) for path in outputs)
    worker_temp = config.storage.temp_root / f"tracks-{partition:04d}"
    deduplicated_path = worker_temp / "deduplicated.parquet"
    conflict_path = worker_temp / "time_conflicts.parquet"
    phase_total = 5
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)

    source_bytes = estimated_source_bytes
    if source_bytes is None:
        source_bytes = total_file_size(dynamic_paths) // config.layout.track_partitions
    source_bytes = max(int(source_bytes), 1024**2)
    materialized_bytes = shared_output_requirement(config, source_bytes * 2)
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config) + materialized_bytes,
        f"track partition {partition:04d} temporary data",
    )
    ensure_space(
        track_output,
        config.storage.reserves_gib["tracks"],
        source_bytes * 2,
        f"track partition {partition:04d}",
    )
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        shared_output_requirement(config, source_bytes),
        f"track products {partition:04d}",
    )

    try:
        ensure_space(
            config.storage.temp_root,
            config.storage.reserves_gib["temp"],
            shared_temp_requirement(config) + materialized_bytes,
            f"track partition {partition:04d} exact deduplication",
        )
        deduplicated_rows = _copy_phase(
            config,
            worker_temp,
            heartbeat_path,
            phase_index=1,
            phase_total=phase_total,
            phase_key="deduplicate",
            label="exact deduplication",
            select_sql=_deduplicated_sql(dynamic_paths, partition),
            output_path=deduplicated_path,
            order_by=None,
            space_path=config.storage.temp_root,
        )

        ensure_space(
            config.storage.temp_root,
            config.storage.reserves_gib["temp"],
            shared_temp_requirement(config) + materialized_bytes,
            f"track partition {partition:04d} conflict index",
        )
        conflict_rows = _copy_phase(
            config,
            worker_temp,
            heartbeat_path,
            phase_index=2,
            phase_total=phase_total,
            phase_key="conflicts",
            label="time conflict index",
            select_sql=_conflict_sql(deduplicated_path),
            output_path=conflict_path,
            order_by=None,
            space_path=config.storage.temp_root,
        )

        ensure_space(
            track_output,
            config.storage.reserves_gib["tracks"],
            source_bytes * 2,
            f"track partition {partition:04d} canonical output",
        )
        track_rows = _copy_phase(
            config,
            worker_temp,
            heartbeat_path,
            phase_index=3,
            phase_total=phase_total,
            phase_key="sequence",
            label="trajectory sequencing",
            select_sql=_sequenced_sql(config, deduplicated_path, conflict_path),
            output_path=temporary_outputs[0],
            order_by="mmsi, point_seq",
            space_path=track_output,
        )

        ensure_space(
            config.storage.products_root,
            config.storage.reserves_gib["products"],
            shared_output_requirement(config, source_bytes),
            f"track partition {partition:04d} stop events",
        )
        stop_rows = _copy_phase(
            config,
            worker_temp,
            heartbeat_path,
            phase_index=4,
            phase_total=phase_total,
            phase_key="stops",
            label="stop event detection",
            select_sql=_stop_sql(
                config,
                partition,
                parquet_sources([temporary_outputs[0]]),
            ),
            output_path=temporary_outputs[1],
            order_by="mmsi, start_time_utc",
            space_path=config.storage.products_root,
        )

        ensure_space(
            config.storage.products_root,
            config.storage.reserves_gib["products"],
            shared_output_requirement(config, source_bytes),
            f"track partition {partition:04d} vessel static output",
        )
        vessel_rows = _copy_phase(
            config,
            worker_temp,
            heartbeat_path,
            phase_index=5,
            phase_total=phase_total,
            phase_key="vessels",
            label="vessel static compaction",
            select_sql=_vessel_sql(static_paths, partition),
            output_path=temporary_outputs[2],
            order_by="mmsi",
            space_path=config.storage.products_root,
        )
    finally:
        shutil.rmtree(worker_temp, ignore_errors=True)

    heartbeat(heartbeat_path, "committing")
    for temporary, output in zip(temporary_outputs, outputs):
        atomic_replace(temporary, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": track_rows,
        "deduplicated_count": deduplicated_rows,
        "conflict_count": conflict_rows,
        "stop_count": stop_rows,
        "vessel_count": vessel_rows,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def _partition_weights(config: AppConfig, inventory: Inventory) -> dict[int, int]:
    stats = [stats_run_path(config, item.date) for item in inventory.files]
    total_dynamic_bytes = sum(
        dynamic_run_path(config, lane, item.date).stat().st_size
        for item in inventory.files
        for lane in range(len(config.storage.track_roots))
    )
    rows = {partition: 0 for partition in range(config.layout.track_partitions)}
    if stats:
        result = duckdb.sql(
            f"""
            SELECT track_partition_id, sum(row_count)::BIGINT
            FROM {parquet_sources(stats)}
            GROUP BY track_partition_id
            """
        ).fetchall()
        rows.update({int(partition): int(count) for partition, count in result})
    total_rows = sum(rows.values())
    if total_rows == 0:
        return {partition: 1 for partition in rows}
    return {
        partition: max(1, round(total_dynamic_bytes * count / total_rows))
        for partition, count in rows.items()
    }


def build_tracks(
    config: AppConfig,
    inventory: Inventory,
    inventory_signature: str,
    store: CheckpointStore,
) -> None:
    lane_count = len(config.storage.track_roots)
    dynamic_by_lane = {
        lane: [dynamic_run_path(config, lane, item.date) for item in inventory.files]
        for lane in range(lane_count)
    }
    static_paths = [static_run_path(config, item.date) for item in inventory.files]
    weights = _partition_weights(config, inventory)
    stage_hash = config.section_hash("layout", "cleaning", "stops")
    tasks: list[StageTask] = []
    for partition in range(config.layout.track_partitions):
        lane = partition % lane_count
        outputs = (
            track_path(config, partition),
            stop_path(config, partition),
            vessel_path(config, partition),
        )
        task_signature = signature([stage_hash, inventory_signature, partition])
        heartbeat_path = (
            config.storage.temp_root / "heartbeats" / f"tracks-{partition:04d}.json"
        )
        tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=task_signature,
                source_bytes=weights[partition],
                outputs=outputs,
                heartbeat_path=heartbeat_path,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_track_worker,
                    args=(
                        config,
                        partition,
                        dynamic_by_lane[lane],
                        static_paths,
                        outputs,
                        heartbeat_path,
                        weights[partition],
                    ),
                    resource=str(lane_for_partition(config, partition)),
                ),
            )
        )

    def complete(task: StageTask, value: Any, _pid: int, _elapsed: float) -> tuple[int, int]:
        return int(value["output_bytes"]), int(value["row_count"])

    run_stage_tasks(config, store, "tracks", tasks, complete)


def tracks_are_complete(
    config: AppConfig,
    inventory_signature: str,
    store: CheckpointStore,
) -> bool:
    stage_hash = config.section_hash("layout", "cleaning", "stops")
    for partition in range(config.layout.track_partitions):
        outputs = (
            track_path(config, partition),
            stop_path(config, partition),
            vessel_path(config, partition),
        )
        if not store.is_complete(
            "tracks",
            f"{partition:04d}",
            signature([stage_hash, inventory_signature, partition]),
            outputs,
        ):
            return False
    return True


def cleanup_ingest_runs(config: AppConfig) -> None:
    for root in config.storage.track_roots:
        shutil.rmtree(root / "ingest_runs", ignore_errors=True)
    shutil.rmtree(config.storage.products_root / "ingest_runs", ignore_errors=True)
