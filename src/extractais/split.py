from __future__ import annotations

import os
import shutil
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional

from tqdm import tqdm

from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.fileutils import temporary_file
from extractais.gitmeta import git_commit
from extractais.inventory import InputFile
from extractais.manifest import load_split_manifest, write_json_atomic
from extractais.schema import (
    DYNAMIC_SELECT,
    NORMALIZED_PROJECTION,
    RAW_PROJECTION,
    STATIC_SELECT,
    sql_int_list,
)


GIB = 1024**3
TIB = 1024**4


def _output_paths(work_root: Path, item: InputFile) -> tuple[Path, Path]:
    month = item.date[5:7]
    day = item.date[8:10]
    base = work_root / "stage01_split"
    dynamic = (
        base
        / "dynamic"
        / f"year={item.year}"
        / f"month={month}"
        / f"day={day}"
        / "part.parquet"
    )
    static = (
        base
        / "static"
        / f"year={item.year}"
        / f"month={month}"
        / f"day={day}"
        / "part.parquet"
    )
    return dynamic, static


def _free_space_bytes(config: AppConfig) -> int:
    config.storage.work_root.mkdir(parents=True, exist_ok=True)
    return int(shutil.disk_usage(config.storage.work_root).free)


def _enforce_free_space(config: AppConfig, item: InputFile) -> int:
    free_bytes = _free_space_bytes(config)
    required_bytes = int(config.runtime.minimum_free_space_gb * GIB)
    if required_bytes and free_bytes < required_bytes:
        raise RuntimeError(
            f"Free-space guard stopped before {item.date}: "
            f"{free_bytes / GIB:.1f} GiB available, "
            f"{config.runtime.minimum_free_space_gb:.1f} GiB required"
        )
    return free_bytes


def _copy_count(connection, select_sql: str, output: Path, config: AppConfig) -> int:
    result = connection.execute(
        parquet_copy_sql(
            select_sql,
            output,
            config.split.compression,
            config.split.row_group_size,
        )
    ).fetchone()
    return int(result[0])


def _process_day(
    config: AppConfig,
    item: InputFile,
    dynamic_temp: Path,
    static_temp: Path,
) -> Dict[str, Any]:
    connection = open_database(config)
    try:
        raw_sql = RAW_PROJECTION.format(input_path=sql_literal(item.path))
        connection.execute(
            "CREATE TEMP TABLE normalized_day AS "
            f"WITH raw_day AS ({raw_sql}) {NORMALIZED_PROJECTION}"
        )

        dynamic_types = sql_int_list(config.split.dynamic_message_types)
        static_types = sql_int_list(config.split.static_message_types)
        dynamic_sql = DYNAMIC_SELECT.format(dynamic_types=dynamic_types)
        static_sql = STATIC_SELECT.format(static_types=static_types)

        counts = connection.execute(
            f"""
            SELECT
                count(*) AS parsed_rows,
                count(*) FILTER (WHERE msg_type IN ({dynamic_types})) AS dynamic_rows,
                count(*) FILTER (WHERE msg_type IN ({static_types})) AS static_rows,
                count(*) FILTER (
                    WHERE msg_type NOT IN ({dynamic_types}, {static_types})
                       OR msg_type IS NULL
                ) AS other_rows
            FROM normalized_day
            """
        ).fetchone()
        csv_reject_rows = int(
            connection.execute("SELECT count(*) FROM csv_reject_errors").fetchone()[0]
        )
        dynamic_valid_rows = _copy_count(
            connection, dynamic_sql, dynamic_temp, config
        )
        static_valid_rows = _copy_count(connection, static_sql, static_temp, config)
        return {
            "parsed_rows": int(counts[0]),
            "dynamic_message_rows": int(counts[1]),
            "dynamic_valid_rows": dynamic_valid_rows,
            "dynamic_invalid_rows": int(counts[1]) - dynamic_valid_rows,
            "static_message_rows": int(counts[2]),
            "static_valid_rows": static_valid_rows,
            "static_invalid_rows": int(counts[2]) - static_valid_rows,
            "other_rows": int(counts[3]),
            "csv_reject_rows": csv_reject_rows,
        }
    finally:
        connection.close()


def split_files(
    config: AppConfig,
    files: Iterable[InputFile],
    project_root: Path,
    force: bool = False,
    limit_files: Optional[int] = None,
) -> Dict[str, Any]:
    selected = list(files)
    if limit_files is not None:
        selected = selected[:limit_files]

    manifest_path = config.storage.work_root / "manifests" / "split.json"
    manifest = load_split_manifest(manifest_path)
    stage_hash = config.stage_hash("input", "split")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
    manifest["minimum_free_space_gb"] = config.runtime.minimum_free_space_gb
    manifest["updated_at_utc"] = datetime.now(timezone.utc).isoformat()

    total_bytes = sum(item.size_bytes for item in selected)
    progress = tqdm(
        total=total_bytes,
        unit="B",
        unit_scale=True,
        unit_divisor=1024,
        desc="split AIS",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
    )

    try:
        for item in selected:
            dynamic_path, static_path = _output_paths(config.storage.work_root, item)
            previous = manifest["files"].get(item.path)
            complete = (
                previous
                and previous.get("status") == "complete"
                and previous.get("input_identity") == item.identity
                and previous.get("config_hash") == stage_hash
                and dynamic_path.exists()
                and static_path.exists()
            )
            if complete and not force:
                progress.update(item.size_bytes)
                progress.set_postfix_str(f"skip {item.date}")
                continue

            free_before = _enforce_free_space(config, item)
            progress.set_postfix_str(
                f"{item.date} free={free_before / TIB:.2f}TiB"
            )
            started = time.perf_counter()
            dynamic_path.parent.mkdir(parents=True, exist_ok=True)
            static_path.parent.mkdir(parents=True, exist_ok=True)
            dynamic_temp = temporary_file(dynamic_path)
            static_temp = temporary_file(static_path)
            dynamic_temp.unlink(missing_ok=True)
            static_temp.unlink(missing_ok=True)

            try:
                statistics = _process_day(
                    config, item, dynamic_temp, static_temp
                )
                os.replace(dynamic_temp, dynamic_path)
                os.replace(static_temp, static_path)

                dynamic_output_bytes = dynamic_path.stat().st_size
                static_output_bytes = static_path.stat().st_size
                total_output_bytes = dynamic_output_bytes + static_output_bytes
                compression_ratio = (
                    total_output_bytes / item.size_bytes if item.size_bytes else 0.0
                )
                free_after = _free_space_bytes(config)
                elapsed = time.perf_counter() - started
                manifest["files"][item.path] = {
                    "status": "complete",
                    "date": item.date,
                    "input_identity": item.identity,
                    "input_size_bytes": item.size_bytes,
                    "config_hash": stage_hash,
                    "dynamic_path": str(dynamic_path.resolve()),
                    "static_path": str(static_path.resolve()),
                    **statistics,
                    "dynamic_output_bytes": dynamic_output_bytes,
                    "static_output_bytes": static_output_bytes,
                    "total_output_bytes": total_output_bytes,
                    "compression_ratio": round(compression_ratio, 6),
                    "free_space_bytes_after": free_after,
                    "elapsed_seconds": round(elapsed, 3),
                    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                write_json_atomic(manifest_path, manifest)
                progress.set_postfix_str(
                    f"{item.date} ratio={compression_ratio:.1%} "
                    f"free={free_after / TIB:.2f}TiB"
                )
            except Exception as exc:
                manifest["files"][item.path] = {
                    "status": "failed",
                    "date": item.date,
                    "input_identity": item.identity,
                    "config_hash": stage_hash,
                    "error": f"{type(exc).__name__}: {exc}",
                    "failed_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                write_json_atomic(manifest_path, manifest)
                raise
            finally:
                dynamic_temp.unlink(missing_ok=True)
                static_temp.unlink(missing_ok=True)

            progress.update(item.size_bytes)
    finally:
        progress.close()

    return manifest
