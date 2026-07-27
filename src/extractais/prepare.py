from __future__ import annotations

import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable

from tqdm import tqdm

from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.fileutils import (
    remove_path,
    replace_directory,
    replace_file,
    temporary_directory,
    temporary_file,
)
from extractais.gitmeta import git_commit
from extractais.isolated import IsolatedCall, run_isolated, run_isolated_many
from extractais.manifest import read_json
from extractais.sql import haversine_km, parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)
from extractais.storage import (
    GIB,
    TIB,
    directory_stats,
    ensure_storage_budget,
    ensure_parallel_storage_budget,
    free_space_bytes,
    total_file_size,
)
from extractais.progress import HONEST_BAR_FORMAT, ProgressEstimator, format_duration


def _split_records(config: AppConfig) -> list[Dict[str, Any]]:
    split_path = config.storage.work_root / "manifests" / "split.json"
    split = read_json(split_path, {"files": {}})
    records = [
        record
        for record in split.get("files", {}).values()
        if record.get("status") == "complete"
        and Path(record.get("dynamic_path", "")).exists()
        and Path(record.get("static_path", "")).exists()
    ]
    if not records:
        raise RuntimeError("No completed split outputs found; run `extractais split` first")
    return sorted(records, key=lambda row: row["date"])


def _monthly_records(records: Iterable[Dict[str, Any]]) -> Dict[str, list[Dict[str, Any]]]:
    groups: Dict[str, list[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[record["date"][:7]].append(record)
    return dict(sorted(groups.items()))


def _copy_partitioned_month(
    config: AppConfig, paths: list[Path], temporary: Path
) -> int:
    source = parquet_sources(paths)
    output_reserve_bytes = total_file_size(paths) * 2
    select_sql = f"""
        SELECT
            *,
            cast(mmsi % {config.prepare.mmsi_buckets} AS INTEGER) AS mmsi_bucket
        FROM {source}
    """
    query = f"""
        COPY ({select_sql})
        TO {sql_literal(str(temporary.resolve()))}
        (
            FORMAT PARQUET,
            PARTITION_BY (mmsi_bucket),
            COMPRESSION {config.prepare.compression.upper()},
            ROW_GROUP_SIZE {config.prepare.row_group_size}
        )
    """
    connection = open_database(config, output_reserve_bytes=output_reserve_bytes)
    try:
        return int(connection.execute(query).fetchone()[0])
    finally:
        connection.close()


def _compact_static(config: AppConfig, paths: list[Path], output: Path) -> int:
    source = parquet_sources(paths)
    output_reserve_bytes = total_file_size(paths)
    select_sql = f"""
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
            arg_max(to_bow, timestamp_utc) FILTER (WHERE to_bow IS NOT NULL) AS to_bow,
            arg_max(to_stern, timestamp_utc) FILTER (WHERE to_stern IS NOT NULL) AS to_stern,
            arg_max(to_port, timestamp_utc) FILTER (WHERE to_port IS NOT NULL) AS to_port,
            arg_max(to_starboard, timestamp_utc)
                FILTER (WHERE to_starboard IS NOT NULL) AS to_starboard,
            arg_max(ais_version, timestamp_utc)
                FILTER (WHERE ais_version IS NOT NULL) AS ais_version,
            arg_max(ship_type, timestamp_utc)
                FILTER (WHERE ship_type IS NOT NULL) AS ship_type,
            arg_max(collection_type, timestamp_utc)
                FILTER (WHERE collection_type IS NOT NULL) AS collection_type,
            arg_max(source, timestamp_utc) FILTER (WHERE source IS NOT NULL) AS source
        FROM {source}
        GROUP BY mmsi
        ORDER BY mmsi
    """
    temporary = temporary_file(output)
    temporary.unlink(missing_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(config, output_reserve_bytes=output_reserve_bytes)
    try:
        count = int(
            connection.execute(
                parquet_copy_sql(
                    select_sql,
                    temporary,
                    config.prepare.compression,
                    config.prepare.row_group_size,
                )
            ).fetchone()[0]
        )
        replace_file(temporary, output)
        return count
    finally:
        temporary.unlink(missing_ok=True)
        connection.close()


def _bucket_files(partition_root: Path) -> Dict[int, list[Path]]:
    result: Dict[int, list[Path]] = defaultdict(list)
    for path in partition_root.rglob("*.parquet"):
        for part in path.parts:
            if part.startswith("mmsi_bucket="):
                result[int(part.split("=", 1)[1])].append(path)
                break
    return {bucket: sorted(paths) for bucket, paths in result.items()}


def _write_track_bucket(
    config: AppConfig, paths: list[Path], bucket: int, output: Path
) -> int:
    source = parquet_sources(paths)
    output_reserve_bytes = total_file_size(paths) * 2
    distance = haversine_km("prev_latitude", "prev_longitude", "latitude", "longitude")
    select_sql = f"""
        WITH ranked AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY mmsi, timestamp_utc, latitude, longitude
                    ORDER BY source_at_dock DESC NULLS LAST, msg_type, msg_id NULLS LAST
                ) AS exact_rank
            FROM {source}
        ),
        deduplicated AS (
            SELECT
                * EXCLUDE (exact_rank, mmsi_bucket),
                count(*) OVER (PARTITION BY mmsi, timestamp_utc) > 1 AS is_time_conflict
            FROM ranked
            WHERE exact_rank = 1
        ),
        sequenced AS (
            SELECT
                *,
                row_number() OVER (
                    PARTITION BY mmsi ORDER BY timestamp_utc, latitude, longitude
                ) AS point_seq,
                lag(timestamp_utc) OVER (
                    PARTITION BY mmsi ORDER BY timestamp_utc, latitude, longitude
                ) AS prev_timestamp_utc,
                lag(latitude) OVER (
                    PARTITION BY mmsi ORDER BY timestamp_utc, latitude, longitude
                ) AS prev_latitude,
                lag(longitude) OVER (
                    PARTITION BY mmsi ORDER BY timestamp_utc, latitude, longitude
                ) AS prev_longitude
            FROM deduplicated
        ),
        measured AS (
            SELECT
                *,
                date_diff('second', prev_timestamp_utc, timestamp_utc) AS gap_seconds,
                CASE WHEN prev_timestamp_utc IS NOT NULL THEN {distance} END AS step_distance_km
            FROM sequenced
        ),
        flagged AS (
            SELECT
                *,
                CASE
                    WHEN gap_seconds > 0
                    THEN step_distance_km / gap_seconds * 3600.0 / 1.852
                END AS implied_speed_knots
            FROM measured
        )
        SELECT
            * EXCLUDE (prev_timestamp_utc, prev_latitude, prev_longitude),
            coalesce(implied_speed_knots > {config.prepare.max_implied_speed_knots}, false)
                AS is_kinematic_outlier,
            {bucket}::INTEGER AS mmsi_bucket
        FROM flagged
        ORDER BY mmsi, point_seq
    """
    temporary = temporary_file(output)
    temporary.unlink(missing_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(
        config,
        output_reserve_bytes=output_reserve_bytes,
        workload="bucket",
    )
    try:
        count = int(
            connection.execute(
                parquet_copy_sql(
                    select_sql,
                    temporary,
                    config.prepare.compression,
                    config.prepare.row_group_size,
                )
            ).fetchone()[0]
        )
        replace_file(temporary, output)
        return count
    finally:
        temporary.unlink(missing_ok=True)
        connection.close()


def prepare_data(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    records = _split_records(config)
    months = _monthly_records(records)
    manifest_path = config.storage.work_root / "manifests" / "prepare.json"
    manifest = load_stage_manifest(manifest_path, "prepare")
    stage_hash = config.stage_hash("prepare")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)

    partition_root = config.storage.work_root / "stage02_partitioned"
    month_work: list[Dict[str, Any]] = []
    month_estimator = ProgressEstimator(max_workers=1)
    completed_months = 0
    for month, month_records in months.items():
        source_signature = signature(
            (record["date"], record["input_identity"], record["config_hash"])
            for record in month_records
        )
        year, month_number = month.split("-")
        output = partition_root / f"year={year}" / f"month={month_number}"
        key = f"partition:{month}"
        source_bytes = total_file_size(
            Path(record["dynamic_path"]) for record in month_records
        )
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, [output]
        ):
            completed_months += 1
            item = manifest["items"][key]
            month_estimator.add_sample(
                int(item.get("source_bytes", source_bytes)),
                float(item.get("elapsed_seconds", 0)),
            )
            continue
        month_work.append(
            {
                "month": month,
                "records": month_records,
                "source_signature": source_signature,
                "output": output,
                "key": key,
                "source_bytes": source_bytes,
            }
        )

    month_remaining_bytes = sum(item["source_bytes"] for item in month_work)
    month_progress = tqdm(
        total=len(months),
        initial=completed_months,
        desc="partition months",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
        bar_format=HONEST_BAR_FORMAT,
    )
    for item in month_work:
        month = item["month"]
        month_records = item["records"]
        source_bytes = item["source_bytes"]
        output = item["output"]
        estimated_output_bytes = source_bytes * 2
        free_before = ensure_storage_budget(
            config,
            f"partition month {month}",
            estimated_output_bytes,
        )
        month_progress.set_postfix_str(
            f"{month} {month_estimator.format_eta(month_remaining_bytes)} "
            f"free={free_before / TIB:.2f}TiB"
        )
        started = time.perf_counter()
        temporary = temporary_directory(output)
        remove_path(temporary, config.storage.work_root)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        heartbeat_checked_at = 0.0
        heartbeat_output_bytes = 0
        heartbeat_file_count = 0

        def poll_month(active, unit=month, unit_started=started) -> None:
            nonlocal heartbeat_checked_at
            nonlocal heartbeat_output_bytes
            nonlocal heartbeat_file_count
            now = time.perf_counter()
            if now - heartbeat_checked_at >= 30:
                heartbeat_output_bytes, heartbeat_file_count = directory_stats(
                    temporary
                )
                heartbeat_checked_at = now
            month_progress.set_postfix_str(
                f"{unit} elapsed={format_duration(now - unit_started)} "
                f"out={heartbeat_output_bytes / GIB:.2f}GiB "
                f"files={heartbeat_file_count} "
                f"{month_estimator.format_eta(month_remaining_bytes)}"
            )
            month_progress.refresh()

        worker = run_isolated(
            _copy_partitioned_month,
            config,
            [Path(record["dynamic_path"]) for record in month_records],
            temporary,
            _on_poll=poll_month,
        )
        output_bytes, output_file_count = directory_stats(temporary)
        if output_file_count > config.prepare.mmsi_buckets:
            raise RuntimeError(
                f"Partition file multiplication detected for {month}: "
                f"{output_file_count} files for "
                f"{config.prepare.mmsi_buckets} MMSI buckets"
            )
        replace_directory(temporary, output, config.storage.work_root)
        elapsed = time.perf_counter() - started
        free_after = free_space_bytes(config.storage.work_root)
        io_mib_per_second = (
            (source_bytes + output_bytes) / 1024**2 / elapsed if elapsed else 0.0
        )
        manifest["items"][item["key"]] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": item["source_signature"],
            "output": str(output.resolve()),
            "source_bytes": source_bytes,
            "output_bytes": output_bytes,
            "output_file_count": output_file_count,
            "row_count": worker.value,
            "io_mib_per_second": round(io_mib_per_second, 2),
            "free_space_bytes_before": free_before,
            "free_space_bytes_after": free_after,
            "worker_process_id": worker.process_id,
            "elapsed_seconds": round(elapsed, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
        month_estimator.add_sample(source_bytes, elapsed)
        month_remaining_bytes -= source_bytes
        month_progress.update(1)
        month_progress.set_postfix_str(
            f"{month} {io_mib_per_second:.1f}MiB/s "
            f"{month_estimator.format_eta(month_remaining_bytes)} "
            f"free={free_after / TIB:.2f}TiB"
        )
    month_progress.close()

    static_paths = [Path(record["static_path"]) for record in records]
    static_signature = signature(
        (record["date"], record["input_identity"], record["config_hash"])
        for record in records
    )
    static_output = config.storage.work_root / "stage02_static" / "vessels.parquet"
    if force or not item_is_complete(
        manifest, "static", stage_hash, static_signature, [static_output]
    ):
        source_bytes = total_file_size(static_paths)
        free_before = ensure_storage_budget(
            config,
            "compact static AIS",
            source_bytes,
        )
        started = time.perf_counter()
        static_progress = tqdm(
            total=1,
            desc="compact static AIS",
            disable=not config.runtime.enable_progress,
            dynamic_ncols=True,
            bar_format=HONEST_BAR_FORMAT,
        )

        def poll_static(active) -> None:
            static_progress.set_postfix_str(
                f"elapsed={format_duration(time.perf_counter() - started)} "
                "ETA calibrating"
            )
            static_progress.refresh()

        worker = run_isolated(
            _compact_static,
            config,
            static_paths,
            static_output,
            _on_poll=poll_static,
        )
        vessel_count = worker.value
        elapsed = time.perf_counter() - started
        output_bytes = static_output.stat().st_size
        free_after = free_space_bytes(config.storage.work_root)
        manifest["items"]["static"] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": static_signature,
            "output": str(static_output.resolve()),
            "vessel_count": vessel_count,
            "source_bytes": source_bytes,
            "output_bytes": output_bytes,
            "free_space_bytes_before": free_before,
            "free_space_bytes_after": free_after,
            "worker_process_id": worker.process_id,
            "elapsed_seconds": round(elapsed, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
        static_progress.update(1)
        static_progress.set_postfix_str(
            f"elapsed={format_duration(elapsed)} free={free_after / TIB:.2f}TiB"
        )
        static_progress.close()

    bucket_sources = _bucket_files(partition_root)
    tracks_root = config.storage.work_root / "stage03_tracks"
    bucket_work: list[Dict[str, Any]] = []
    bucket_estimator = ProgressEstimator(
        max_workers=config.runtime.bucket_workers
    )
    completed_buckets = 0
    for bucket, paths in sorted(bucket_sources.items()):
        source_signature = signature(
            (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
            for path in paths
        )
        output = tracks_root / f"mmsi_bucket={bucket:04d}" / "part.parquet"
        key = f"track:{bucket:04d}"
        source_bytes = total_file_size(paths)
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, [output]
        ):
            completed_buckets += 1
            item = manifest["items"][key]
            bucket_estimator.add_sample(
                int(item.get("source_bytes", source_bytes)),
                float(item.get("elapsed_seconds", 0)),
            )
            continue
        bucket_work.append(
            {
                "bucket": bucket,
                "paths": paths,
                "source_signature": source_signature,
                "output": output,
                "key": key,
                "source_bytes": source_bytes,
                "estimated_output_bytes": source_bytes * 2,
            }
        )

    bucket_remaining_bytes = sum(item["source_bytes"] for item in bucket_work)
    bucket_remaining_count = len(bucket_work)
    bucket_progress = tqdm(
        total=len(bucket_sources),
        initial=completed_buckets,
        desc="sort MMSI buckets",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
        bar_format=HONEST_BAR_FORMAT,
    )
    work_by_key = {item["key"]: item for item in bucket_work}
    free_before_by_key: Dict[str, int] = {}

    def before_bucket_start(call, active) -> None:
        item = work_by_key[call.key]
        active_output = sum(
            work_by_key[key]["estimated_output_bytes"] for key in active
        )
        free_before_by_key[call.key] = ensure_parallel_storage_budget(
            config,
            f"sort MMSI bucket {item['bucket']:04d}",
            active_output,
            item["estimated_output_bytes"],
        )

    def poll_buckets(active) -> None:
        active_text = ",".join(
            f"{work_by_key[key]['bucket']:04d}" for key in sorted(active)
        )
        bucket_progress.set_postfix_str(
            f"active={active_text or '-'} "
            f"{bucket_estimator.format_eta(bucket_remaining_bytes, bucket_remaining_count)}"
        )
        bucket_progress.refresh()

    calls = [
        IsolatedCall(
            key=item["key"],
            target=_write_track_bucket,
            args=(
                config,
                item["paths"],
                item["bucket"],
                item["output"],
            ),
        )
        for item in bucket_work
    ]
    for result in run_isolated_many(
        calls,
        max_workers=config.runtime.bucket_workers,
        on_poll=poll_buckets,
        before_start=before_bucket_start,
    ):
        item = work_by_key[result.key]
        source_bytes = item["source_bytes"]
        output_bytes = item["output"].stat().st_size
        free_after = free_space_bytes(config.storage.work_root)
        io_mib_per_second = (
            (source_bytes + output_bytes) / 1024**2 / result.elapsed_seconds
            if result.elapsed_seconds
            else 0.0
        )
        manifest["items"][result.key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": item["source_signature"],
            "output": str(item["output"].resolve()),
            "track_row_count": result.value,
            "source_bytes": source_bytes,
            "output_bytes": output_bytes,
            "io_mib_per_second": round(io_mib_per_second, 2),
            "free_space_bytes_before": free_before_by_key[result.key],
            "free_space_bytes_after": free_after,
            "worker_process_id": result.process_id,
            "elapsed_seconds": round(result.elapsed_seconds, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
        bucket_estimator.add_sample(source_bytes, result.elapsed_seconds)
        bucket_remaining_bytes -= source_bytes
        bucket_remaining_count -= 1
        bucket_progress.update(1)
        bucket_progress.set_postfix_str(
            f"{item['bucket']:04d} {io_mib_per_second:.1f}MiB/s "
            f"{bucket_estimator.format_eta(bucket_remaining_bytes, bucket_remaining_count)} "
            f"free={free_after / TIB:.2f}TiB"
        )
    bucket_progress.close()
    return manifest
