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
from extractais.manifest import read_json
from extractais.sql import haversine_km, parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)


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
) -> None:
    source = parquet_sources(paths)
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
    connection = open_database(config)
    try:
        connection.execute(query)
    finally:
        connection.close()


def _compact_static(config: AppConfig, paths: list[Path], output: Path) -> int:
    source = parquet_sources(paths)
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
    connection = open_database(config)
    try:
        count = int(connection.execute(f"SELECT count(DISTINCT mmsi) FROM {source}").fetchone()[0])
        connection.execute(
            parquet_copy_sql(
                select_sql,
                temporary,
                config.prepare.compression,
                config.prepare.row_group_size,
            )
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
            *,
            coalesce(implied_speed_knots > {config.prepare.max_implied_speed_knots}, false)
                AS is_kinematic_outlier,
            {bucket}::INTEGER AS mmsi_bucket
        FROM flagged
        ORDER BY mmsi, point_seq
    """
    temporary = temporary_file(output)
    temporary.unlink(missing_ok=True)
    output.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(config)
    try:
        count = int(connection.execute(f"SELECT count(*) FROM {source}").fetchone()[0])
        connection.execute(
            parquet_copy_sql(
                select_sql,
                temporary,
                config.prepare.compression,
                config.prepare.row_group_size,
            )
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
    month_progress = tqdm(
        months.items(),
        total=len(months),
        desc="partition months",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
    )
    for month, month_records in month_progress:
        source_signature = signature(
            (record["date"], record["input_identity"], record["config_hash"])
            for record in month_records
        )
        year, month_number = month.split("-")
        output = partition_root / f"year={year}" / f"month={month_number}"
        key = f"partition:{month}"
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, [output]
        ):
            continue

        started = time.perf_counter()
        temporary = temporary_directory(output)
        remove_path(temporary, config.storage.work_root)
        temporary.parent.mkdir(parents=True, exist_ok=True)
        _copy_partitioned_month(
            config,
            [Path(record["dynamic_path"]) for record in month_records],
            temporary,
        )
        replace_directory(temporary, output, config.storage.work_root)
        manifest["items"][key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": source_signature,
            "output": str(output.resolve()),
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)

    static_paths = [Path(record["static_path"]) for record in records]
    static_signature = signature(
        (record["date"], record["input_identity"], record["config_hash"])
        for record in records
    )
    static_output = config.storage.work_root / "stage02_static" / "vessels.parquet"
    if force or not item_is_complete(
        manifest, "static", stage_hash, static_signature, [static_output]
    ):
        started = time.perf_counter()
        vessel_count = _compact_static(config, static_paths, static_output)
        manifest["items"]["static"] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": static_signature,
            "output": str(static_output.resolve()),
            "vessel_count": vessel_count,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)

    bucket_sources = _bucket_files(partition_root)
    tracks_root = config.storage.work_root / "stage03_tracks"
    bucket_progress = tqdm(
        sorted(bucket_sources.items()),
        total=len(bucket_sources),
        desc="sort MMSI buckets",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
    )
    for bucket, paths in bucket_progress:
        source_signature = signature(
            (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
            for path in paths
        )
        output = tracks_root / f"mmsi_bucket={bucket:04d}" / "part.parquet"
        key = f"track:{bucket:04d}"
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, [output]
        ):
            continue
        started = time.perf_counter()
        source_rows = _write_track_bucket(config, paths, bucket, output)
        manifest["items"][key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": source_signature,
            "output": str(output.resolve()),
            "source_rows": source_rows,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
    return manifest
