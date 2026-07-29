from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from extractais.bucketstage import BucketExecution, BucketTask, run_bucket_stage
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.fileutils import remove_path, replace_file, temporary_file
from extractais.gitmeta import git_commit
from extractais.isolated import IsolatedCall
from extractais.sql import haversine_km, parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)
from extractais.storage import free_space_bytes


def track_bucket_files(config: AppConfig) -> list[Path]:
    paths = sorted((config.storage.work_root / "stage03_tracks").glob("mmsi_bucket=*/part.parquet"))
    if not paths:
        raise RuntimeError("No sorted track buckets found; run `extractais prepare` first")
    return paths


def bucket_number(path: Path) -> int:
    return int(path.parent.name.split("=", 1)[1])


def _write_stop_bucket(config: AppConfig, source_path: Path, output: Path) -> int:
    source = parquet_sources([source_path])
    diameter = haversine_km("min_latitude", "min_longitude", "max_latitude", "max_longitude")
    max_gap_seconds = config.stops.max_point_gap_minutes * 60
    select_sql = f"""
        WITH candidates AS (
            SELECT
                mmsi, point_seq, timestamp_utc, latitude, longitude, speed,
                source_at_dock, gap_seconds, step_distance_km,
                (
                    coalesce(speed <= {config.stops.max_speed_knots}, false)
                    OR (speed IS NULL AND coalesce(step_distance_km <= {config.stops.max_step_km}, false))
                ) AS is_stop_candidate
            FROM {source}
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
                CASE
                    WHEN is_stop_candidate AND (
                        NOT previous_is_stop_candidate
                        OR gap_seconds IS NULL
                        OR gap_seconds > {max_gap_seconds}
                    ) THEN 1
                    ELSE 0
                END AS new_stop_run
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
                date_diff('second', min(timestamp_utc), max(timestamp_utc)) AS duration_seconds,
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
            {bucket_number(source_path)}::INTEGER AS mmsi_bucket
        FROM measured
        WHERE duration_seconds >= {config.stops.min_duration_minutes * 60}
          AND diameter_km <= {config.stops.max_diameter_km}
        ORDER BY mmsi, start_time_utc
    """
    temporary = temporary_file(output)
    worker_temp = (
        config.storage.temp_directory
        / f"stops-bucket-{bucket_number(source_path):04d}"
    )
    remove_path(worker_temp, config.storage.temp_directory)
    temporary.unlink(missing_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    try:
        connection = open_database(
            config,
            output_reserve_bytes=source_path.stat().st_size,
            workload="bucket",
            worker_temp_directory=worker_temp,
        )
        try:
            result = connection.execute(
                parquet_copy_sql(
                    select_sql,
                    temporary,
                    config.prepare.compression,
                    config.prepare.row_group_size,
                )
            ).fetchone()
            count = int(result[0])
        finally:
            connection.close()
        replace_file(temporary, output)
        return count
    finally:
        temporary.unlink(missing_ok=True)
        remove_path(worker_temp, config.storage.temp_directory)


def build_stops(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    sources = track_bucket_files(config)
    manifest_path = config.storage.work_root / "manifests" / "stops.json"
    manifest = load_stage_manifest(manifest_path, "stops")
    stage_hash = config.stage_hash("stops", "prepare")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)

    tasks: list[BucketTask] = []
    work: Dict[str, Dict[str, Any]] = {}
    completed_samples: list[tuple[int, float]] = []
    completed_count = 0
    for source_path in sources:
        bucket = bucket_number(source_path)
        source_signature = signature(
            [(str(source_path.resolve()), source_path.stat().st_size, source_path.stat().st_mtime_ns)]
        )
        output = (
            config.storage.work_root
            / "stage04_stops"
            / f"mmsi_bucket={bucket:04d}"
            / "part.parquet"
        )
        key = f"bucket:{bucket:04d}"
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, [output]
        ):
            completed_count += 1
            item = manifest["items"][key]
            completed_samples.append(
                (
                    int(item.get("source_bytes", source_path.stat().st_size)),
                    float(item.get("elapsed_seconds", 0)),
                )
            )
            continue
        source_bytes = source_path.stat().st_size
        work[key] = {
            "bucket": bucket,
            "source_signature": source_signature,
            "source_bytes": source_bytes,
            "output": output,
        }
        tasks.append(
            BucketTask(
                key=key,
                label=f"{bucket:04d}",
                source_bytes=source_bytes,
                estimated_output_bytes=source_bytes,
                call=IsolatedCall(
                    key=key,
                    target=_write_stop_bucket,
                    args=(config, source_path, output),
                ),
            )
        )

    def complete(execution: BucketExecution) -> str:
        item = work[execution.task.key]
        output = item["output"]
        free_after = free_space_bytes(config.storage.work_root)
        output_bytes = output.stat().st_size
        source_bytes = item["source_bytes"]
        elapsed = execution.result.elapsed_seconds
        io_rate = (source_bytes + output_bytes) / 1024**2 / elapsed
        manifest["items"][execution.task.key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": item["source_signature"],
            "output": str(output.resolve()),
            "stop_count": execution.result.value,
            "source_bytes": source_bytes,
            "output_bytes": output_bytes,
            "io_mib_per_second": round(io_rate, 2),
            "free_space_bytes_before": execution.free_space_bytes_before,
            "free_space_bytes_after": free_after,
            "worker_process_id": execution.result.process_id,
            "elapsed_seconds": round(elapsed, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
        return f"{io_rate:.1f}MiB/s"

    run_bucket_stage(
        config,
        "detect stop events",
        tasks,
        total_count=len(sources),
        completed_count=completed_count,
        completed_samples=completed_samples,
        on_complete=complete,
    )
    return manifest
