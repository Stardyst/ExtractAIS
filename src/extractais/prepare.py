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
from extractais.manifest import read_json, write_json_atomic
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
    parse_size_bytes,
    total_file_size,
)
from extractais.progress import (
    HONEST_BAR_FORMAT,
    FRACTIONAL_BAR_FORMAT,
    ProgressEstimator,
    advance_progress_to,
    format_duration,
)


TRACK_LAYOUT_VERSION = 2
TRACK_SHARD_TARGET_BYTES = 512 * 1024**2
TRACK_MIN_SHARDS_PER_SOURCE_BUCKET = 8
TRACK_MAX_SHARDS_PER_SOURCE_BUCKET = 64
TRACK_SHARD_STRIDE = TRACK_MAX_SHARDS_PER_SOURCE_BUCKET
TRACK_RESHARD_WEIGHT = 0.10


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


def _copy_track_phase(
    config: AppConfig,
    select_sql: str,
    output: Path,
    output_reserve_bytes: int,
    duckdb_temp: Path,
) -> int:
    connection = open_database(
        config,
        output_reserve_bytes=output_reserve_bytes,
        workload="bucket",
        worker_temp_directory=duckdb_temp,
        threads_override=max(
            1,
            min(
                config.runtime.bucket_threads,
                parse_size_bytes(config.runtime.bucket_memory_limit)
                // (256 * 1024**2),
            ),
        ),
    )
    try:
        return int(
            connection.execute(
                parquet_copy_sql(
                    select_sql,
                    output,
                    config.prepare.compression,
                    config.prepare.row_group_size,
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()


def _track_bucket_scratch(config: AppConfig, bucket: int) -> Path:
    return config.storage.temp_directory / f"track-bucket-{bucket:04d}"


def _track_shard_count(config: AppConfig, source_bytes: int) -> int:
    memory_target = max(
        256 * 1024,
        parse_size_bytes(config.runtime.bucket_memory_limit)
        // config.runtime.bucket_threads
        // 16,
    )
    target_bytes = min(TRACK_SHARD_TARGET_BYTES, memory_target)
    required = max(
        TRACK_MIN_SHARDS_PER_SOURCE_BUCKET,
        (source_bytes + target_bytes - 1) // target_bytes,
    )
    power_of_two = 1 << (required - 1).bit_length()
    return min(TRACK_MAX_SHARDS_PER_SOURCE_BUCKET, power_of_two)


def _track_work_unit(source_bucket: int, local_shard: int) -> int:
    return source_bucket * TRACK_SHARD_STRIDE + local_shard


def _track_output(tracks_root: Path, work_unit: int) -> Path:
    return tracks_root / f"mmsi_bucket={work_unit:05d}" / "part.parquet"


def _track_layout() -> Dict[str, Any]:
    return {
        "version": TRACK_LAYOUT_VERSION,
        "work_unit": "adaptive_hash_shard",
        "shard_target_bytes": TRACK_SHARD_TARGET_BYTES,
        "minimum_shards_per_source_bucket": TRACK_MIN_SHARDS_PER_SOURCE_BUCKET,
        "maximum_shards_per_source_bucket": TRACK_MAX_SHARDS_PER_SOURCE_BUCKET,
        "work_unit_stride": TRACK_SHARD_STRIDE,
    }


def _write_track_progress(
    path: Path,
    *,
    source_bucket: int,
    phase: str,
    fraction: float,
    completed_shards: int,
    total_shards: int,
    current_shard: int | None = None,
) -> None:
    write_json_atomic(
        path,
        {
            "source_bucket": source_bucket,
            "phase": phase,
            "fraction": min(1.0, max(0.0, fraction)),
            "completed_shards": completed_shards,
            "total_shards": total_shards,
            "current_shard": current_shard,
        },
    )


def _partition_track_source(
    config: AppConfig,
    paths: list[Path],
    output: Path,
    shard_count: int,
    duckdb_temp: Path,
) -> int:
    source = parquet_sources(paths)
    query = f"""
        COPY (
            SELECT
                * EXCLUDE (mmsi_bucket),
                cast(hash(mmsi) % {shard_count} AS INTEGER) AS track_shard
            FROM {source}
        )
        TO {sql_literal(str(output.resolve()))}
        (
            FORMAT PARQUET,
            PARTITION_BY (track_shard),
            COMPRESSION {config.prepare.compression.upper()},
            ROW_GROUP_SIZE {config.prepare.row_group_size}
        )
    """
    connection = open_database(
        config,
        output_reserve_bytes=total_file_size(paths) * 2,
        workload="bucket",
        worker_temp_directory=duckdb_temp,
        threads_override=1,
    )
    try:
        return int(connection.execute(query).fetchone()[0])
    finally:
        connection.close()


def _track_shard_files(root: Path) -> Dict[int, list[Path]]:
    result: Dict[int, list[Path]] = {}
    for directory in root.glob("track_shard=*"):
        if not directory.is_dir():
            continue
        shard = int(directory.name.split("=", 1)[1])
        paths = sorted(directory.rglob("*.parquet"))
        if paths:
            result[shard] = paths
    return result


def _checkpoint_outputs(checkpoint: Dict[str, Any]) -> Dict[int, Dict[str, Any]]:
    valid: Dict[int, Dict[str, Any]] = {}
    for key, item in checkpoint.get("shards", {}).items():
        try:
            shard = int(key)
            output = Path(item["output"])
            output_bytes = int(item["output_bytes"])
        except (KeyError, TypeError, ValueError):
            continue
        if output.exists() and output.stat().st_size == output_bytes:
            valid[shard] = item
    return valid


def _write_track_shard(
    config: AppConfig,
    paths: list[Path],
    source_bucket: int,
    local_shard: int,
    output: Path,
    scratch: Path,
    progress_callback,
) -> int:
    source = parquet_sources(paths)
    output_reserve_bytes = total_file_size(paths) * 4
    work_unit = _track_work_unit(source_bucket, local_shard)
    deduplicated = scratch / "deduplicated.parquet"
    conflicted = scratch / "conflicted.parquet"
    temporary = temporary_file(output)
    remove_path(scratch, config.storage.temp_directory)
    temporary.unlink(missing_ok=True)
    scratch.mkdir(parents=True, exist_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)

    try:
        progress_callback("deduplicating", 0.0)
        deduplication_sql = f"""
            WITH ranked AS (
                SELECT
                    *,
                    row_number() OVER (
                        PARTITION BY mmsi, timestamp_utc, latitude, longitude
                        ORDER BY
                            source_at_dock DESC NULLS LAST,
                            msg_type,
                            msg_id NULLS LAST
                    ) AS exact_rank
                FROM {source}
            )
            SELECT * EXCLUDE (exact_rank)
            FROM ranked
            WHERE exact_rank = 1
        """
        deduplicated_count = _copy_track_phase(
            config,
            deduplication_sql,
            deduplicated,
            output_reserve_bytes,
            scratch / "duckdb-deduplicating",
        )

        progress_callback("detecting conflicts", 1.0 / 3.0)
        conflict_sql = f"""
            SELECT
                *,
                count(*) OVER (PARTITION BY mmsi, timestamp_utc) > 1
                    AS is_time_conflict
            FROM read_parquet({sql_literal(str(deduplicated.resolve()))})
        """
        conflicted_count = _copy_track_phase(
            config,
            conflict_sql,
            conflicted,
            output_reserve_bytes,
            scratch / "duckdb-conflicts",
        )
        if conflicted_count != deduplicated_count:
            raise RuntimeError(
                f"Track row count changed while detecting conflicts for "
                f"work unit {work_unit:05d}: {deduplicated_count} deduplicated rows, "
                f"{conflicted_count} conflict rows"
            )
        deduplicated.unlink()

        progress_callback("sequencing and writing", 2.0 / 3.0)
        distance = haversine_km(
            "prev_latitude", "prev_longitude", "latitude", "longitude"
        )
        sequence_sql = f"""
            WITH sequenced AS (
                SELECT
                    *,
                    row_number() OVER vessel_sequence AS point_seq,
                    lag(timestamp_utc) OVER vessel_sequence AS prev_timestamp_utc,
                    lag(latitude) OVER vessel_sequence AS prev_latitude,
                    lag(longitude) OVER vessel_sequence AS prev_longitude
                FROM read_parquet({sql_literal(str(conflicted.resolve()))})
                WINDOW vessel_sequence AS (
                    PARTITION BY mmsi
                    ORDER BY timestamp_utc, latitude, longitude
                )
            ),
            measured AS (
                SELECT
                    *,
                    date_diff(
                        'second', prev_timestamp_utc, timestamp_utc
                    ) AS gap_seconds,
                    CASE
                        WHEN prev_timestamp_utc IS NOT NULL THEN {distance}
                    END AS step_distance_km
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
                coalesce(
                    implied_speed_knots
                        > {config.prepare.max_implied_speed_knots},
                    false
                ) AS is_kinematic_outlier,
                {work_unit}::INTEGER AS mmsi_bucket
            FROM flagged
            ORDER BY mmsi, point_seq
        """
        count = _copy_track_phase(
            config,
            sequence_sql,
            temporary,
            output_reserve_bytes,
            scratch / "duckdb-sequencing",
        )
        if count != deduplicated_count:
            raise RuntimeError(
                f"Track row count changed while sequencing work unit "
                f"{work_unit:05d}: "
                f"{deduplicated_count} deduplicated rows, {count} sequenced rows"
            )
        progress_callback("committing", 1.0)
        replace_file(temporary, output)
        return count
    finally:
        temporary.unlink(missing_ok=True)
        remove_path(scratch, config.storage.temp_directory)


def _write_track_source_bucket(
    config: AppConfig,
    paths: list[Path],
    source_bucket: int,
    tracks_root: Path,
    source_signature: str,
    track_hash: str,
    force: bool = False,
) -> Dict[str, Any]:
    source_bytes = total_file_size(paths)
    shard_count = _track_shard_count(config, source_bytes)
    scratch = _track_bucket_scratch(config, source_bucket)
    progress_path = scratch / "progress.json"
    partitioned = scratch / "source-shards"
    checkpoint_path = (
        tracks_root
        / "_checkpoints"
        / f"source_bucket={source_bucket:04d}.json"
    )
    identity = {
        "layout_version": TRACK_LAYOUT_VERSION,
        "source_bucket": source_bucket,
        "source_signature": source_signature,
        "track_hash": track_hash,
        "shard_count": shard_count,
    }
    checkpoint = read_json(checkpoint_path, {})
    if force or any(checkpoint.get(key) != value for key, value in identity.items()):
        for local_shard in range(TRACK_SHARD_STRIDE):
            remove_path(
                _track_output(
                    tracks_root, _track_work_unit(source_bucket, local_shard)
                ).parent,
                tracks_root,
            )
        checkpoint = {**identity, "shards": {}}
        write_json_atomic(checkpoint_path, checkpoint)

    completed = _checkpoint_outputs(checkpoint)
    checkpoint_shards = checkpoint.get("shards", {})
    checkpoint_active_count = int(checkpoint.get("active_shard_count", -1))
    if (
        checkpoint.get("status") == "complete"
        and completed
        and len(completed) == len(checkpoint_shards)
        and len(completed) == checkpoint_active_count
    ):
        return {
            "row_count": sum(int(item["row_count"]) for item in completed.values()),
            "outputs": [item["output"] for _, item in sorted(completed.items())],
            "shard_count": shard_count,
            "active_shard_count": len(completed),
        }

    remove_path(scratch, config.storage.temp_directory)
    scratch.mkdir(parents=True, exist_ok=True)
    tracks_root.mkdir(parents=True, exist_ok=True)
    try:
        _write_track_progress(
            progress_path,
            source_bucket=source_bucket,
            phase="resharding source",
            fraction=0.0,
            completed_shards=0,
            total_shards=shard_count,
        )
        partitioned_rows = _partition_track_source(
            config,
            paths,
            partitioned,
            shard_count,
            scratch / "duckdb-resharding",
        )
        shards = _track_shard_files(partitioned)
        if not shards:
            raise RuntimeError(
                f"Track source bucket {source_bucket:04d} produced no shards"
            )

        completed = {
            shard: item for shard, item in completed.items() if shard in shards
        }
        completed_count = len(completed)
        active_count = len(shards)
        _write_track_progress(
            progress_path,
            source_bucket=source_bucket,
            phase="source sharded",
            fraction=TRACK_RESHARD_WEIGHT,
            completed_shards=completed_count,
            total_shards=active_count,
        )

        for ordinal, (local_shard, shard_paths) in enumerate(sorted(shards.items()), 1):
            if local_shard in completed:
                continue

            def report_phase(phase: str, phase_fraction: float) -> None:
                fraction = TRACK_RESHARD_WEIGHT + (1.0 - TRACK_RESHARD_WEIGHT) * (
                    completed_count + phase_fraction
                ) / active_count
                _write_track_progress(
                    progress_path,
                    source_bucket=source_bucket,
                    phase=f"shard {ordinal}/{active_count} {phase}",
                    fraction=fraction,
                    completed_shards=completed_count,
                    total_shards=active_count,
                    current_shard=local_shard,
                )

            output = _track_output(
                tracks_root, _track_work_unit(source_bucket, local_shard)
            )
            row_count = _write_track_shard(
                config,
                shard_paths,
                source_bucket,
                local_shard,
                output,
                scratch / f"work-shard-{local_shard:02d}",
                report_phase,
            )
            completed[local_shard] = {
                "output": str(output.resolve()),
                "output_bytes": output.stat().st_size,
                "row_count": row_count,
            }
            completed_count += 1
            checkpoint = {
                **identity,
                "status": "in_progress",
                "partitioned_row_count": partitioned_rows,
                "active_shard_count": active_count,
                "shards": {
                    str(shard): item for shard, item in sorted(completed.items())
                },
            }
            write_json_atomic(checkpoint_path, checkpoint)
            _write_track_progress(
                progress_path,
                source_bucket=source_bucket,
                phase=f"completed shard {ordinal}/{active_count}",
                fraction=TRACK_RESHARD_WEIGHT
                + (1.0 - TRACK_RESHARD_WEIGHT) * completed_count / active_count,
                completed_shards=completed_count,
                total_shards=active_count,
                current_shard=local_shard,
            )

        checkpoint["status"] = "complete"
        checkpoint["active_shard_count"] = active_count
        checkpoint["completed_at_utc"] = utc_now()
        write_json_atomic(checkpoint_path, checkpoint)
        _write_track_progress(
            progress_path,
            source_bucket=source_bucket,
            phase="complete",
            fraction=1.0,
            completed_shards=active_count,
            total_shards=active_count,
        )
        return {
            "row_count": sum(int(item["row_count"]) for item in completed.values()),
            "outputs": [item["output"] for _, item in sorted(completed.items())],
            "shard_count": shard_count,
            "active_shard_count": active_count,
        }
    finally:
        remove_path(scratch, config.storage.temp_directory)


def _ensure_track_layout(
    config: AppConfig,
    tracks_root: Path,
    manifest: Dict[str, Any],
    manifest_path: Path,
) -> None:
    marker = tracks_root / "_layout.json"
    expected = _track_layout()
    if read_json(marker, {}) == expected:
        return

    remove_path(tracks_root, config.storage.work_root)
    for dependent in (
        "stage04_stops",
        "stage05_ports",
        "stage05_ports.tmp",
        "stage06_port_calls",
        "stage07_intervals",
        "outputs/trajectory_intervals",
        "outputs/validation",
    ):
        remove_path(config.storage.work_root / dependent, config.storage.work_root)
    for stage in ("stops", "ports", "calls", "intervals", "validate"):
        remove_path(
            config.storage.work_root / "manifests" / f"{stage}.json",
            config.storage.work_root,
        )
    tracks_root.mkdir(parents=True, exist_ok=True)
    write_json_atomic(marker, expected)
    manifest["items"] = {
        key: value
        for key, value in manifest.get("items", {}).items()
        if not key.startswith("track:") and not key.startswith("track-source:")
    }
    save_stage_manifest(manifest_path, manifest)


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
    _ensure_track_layout(config, tracks_root, manifest, manifest_path)
    track_hash = signature([stage_hash, _track_layout()])
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
        key = f"track-source:{bucket:04d}"
        source_bytes = total_file_size(paths)
        item = manifest.get("items", {}).get(key, {})
        outputs = [Path(path) for path in item.get("outputs", [])]
        if (
            not force
            and outputs
            and item_is_complete(
                manifest, key, track_hash, source_signature, outputs
            )
        ):
            completed_buckets += 1
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
        desc="build track work units",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
        bar_format=FRACTIONAL_BAR_FORMAT,
    )
    work_by_key = {item["key"]: item for item in bucket_work}
    free_before_by_key: Dict[str, int] = {}
    finished_bucket_progress = float(completed_buckets)

    def before_bucket_start(call, active) -> None:
        item = work_by_key[call.key]
        active_output = sum(
            work_by_key[key]["estimated_output_bytes"] for key in active
        )
        free_before_by_key[call.key] = ensure_parallel_storage_budget(
            config,
            f"build track work units from source bucket {item['bucket']:04d}",
            active_output,
            item["estimated_output_bytes"],
        )

    def poll_buckets(active) -> None:
        active_labels = []
        active_fraction = 0.0
        for key in sorted(active):
            bucket = work_by_key[key]["bucket"]
            phase = active[key].phase
            if phase == "running":
                state = read_json(
                    _track_bucket_scratch(config, bucket) / "progress.json", {}
                )
                phase = str(state.get("phase", "starting"))
                fraction = float(state.get("fraction", 0.0))
                if phase == "resharding source":
                    written_bytes, _ = directory_stats(
                        _track_bucket_scratch(config, bucket) / "source-shards"
                    )
                    source_bytes = work_by_key[key]["source_bytes"]
                    if source_bytes > 0:
                        fraction = max(
                            fraction,
                            TRACK_RESHARD_WEIGHT
                            * min(0.95, written_bytes / source_bytes),
                        )
                active_fraction += fraction
            active_labels.append(f"{bucket:04d}:{phase}")
        advance_progress_to(
            bucket_progress, finished_bucket_progress + active_fraction
        )
        active_text = ",".join(active_labels)
        free_tib = free_space_bytes(config.storage.work_root) / TIB
        bucket_progress.set_postfix_str(
            f"active={active_text or '-'} "
            f"{bucket_estimator.format_eta(bucket_remaining_bytes, bucket_remaining_count)} "
            f"free={free_tib:.2f}TiB"
        )
        bucket_progress.refresh()

    calls = [
        IsolatedCall(
            key=item["key"],
            target=_write_track_source_bucket,
            args=(
                config,
                item["paths"],
                item["bucket"],
                tracks_root,
                item["source_signature"],
                track_hash,
                force,
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
        outputs = [Path(path) for path in result.value["outputs"]]
        output_bytes = total_file_size(outputs)
        free_after = free_space_bytes(config.storage.work_root)
        io_mib_per_second = (
            (source_bytes + output_bytes) / 1024**2 / result.elapsed_seconds
            if result.elapsed_seconds
            else 0.0
        )
        manifest["items"][result.key] = {
            "status": "complete",
            "config_hash": track_hash,
            "source_signature": item["source_signature"],
            "outputs": [str(path.resolve()) for path in outputs],
            "track_row_count": result.value["row_count"],
            "configured_shard_count": result.value["shard_count"],
            "active_shard_count": result.value["active_shard_count"],
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
        finished_bucket_progress += 1.0
        advance_progress_to(bucket_progress, finished_bucket_progress)
        bucket_progress.set_postfix_str(
            f"{item['bucket']:04d} {io_mib_per_second:.1f}MiB/s "
            f"{bucket_estimator.format_eta(bucket_remaining_bytes, bucket_remaining_count)} "
            f"free={free_after / TIB:.2f}TiB"
        )
    bucket_progress.close()
    return manifest
