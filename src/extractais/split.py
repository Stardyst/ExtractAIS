from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, Optional

from tqdm import tqdm

from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.gitmeta import git_commit
from extractais.inventory import InputFile
from extractais.fileutils import temporary_file
from extractais.manifest import load_split_manifest, write_json_atomic
from extractais.schema import (
    DYNAMIC_SELECT,
    NORMALIZED_PROJECTION,
    RAW_PROJECTION,
    STATIC_SELECT,
    sql_int_list,
)


def _output_paths(work_root: Path, item: InputFile) -> tuple[Path, Path]:
    month = item.date[5:7]
    day = item.date[8:10]
    base = work_root / "stage01_split"
    dynamic = base / "dynamic" / f"year={item.year}" / f"month={month}" / f"day={day}" / "part.parquet"
    static = base / "static" / f"year={item.year}" / f"month={month}" / f"day={day}" / "part.parquet"
    return dynamic, static


def _count(connection, sql: str) -> int:
    return int(connection.execute(f"SELECT count(*) FROM ({sql})").fetchone()[0])


def split_files(
    config: AppConfig,
    files: Iterable[InputFile],
    project_root: Path,
    force: bool = False,
    limit_files: Optional[int] = None,
) -> Dict:
    selected = list(files)
    if limit_files is not None:
        selected = selected[:limit_files]

    manifest_path = config.storage.work_root / "manifests" / "split.json"
    manifest = load_split_manifest(manifest_path)
    stage_hash = config.stage_hash("input", "split")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
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

    connection = open_database(config)
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

            progress.set_postfix_str(item.date)
            started = time.perf_counter()
            dynamic_path.parent.mkdir(parents=True, exist_ok=True)
            static_path.parent.mkdir(parents=True, exist_ok=True)
            dynamic_temp = temporary_file(dynamic_path)
            static_temp = temporary_file(static_path)
            dynamic_temp.unlink(missing_ok=True)
            static_temp.unlink(missing_ok=True)

            connection.execute("DROP TABLE IF EXISTS normalized_day")
            connection.execute("DROP TABLE IF EXISTS csv_reject_errors")
            connection.execute("DROP TABLE IF EXISTS csv_reject_scans")

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
                            WHERE msg_type NOT IN ({dynamic_types}, {static_types}) OR msg_type IS NULL
                        ) AS other_rows
                    FROM normalized_day
                    """
                ).fetchone()
                dynamic_valid_rows = _count(connection, dynamic_sql)
                static_valid_rows = _count(connection, static_sql)
                csv_reject_rows = int(
                    connection.execute("SELECT count(*) FROM csv_reject_errors").fetchone()[0]
                )

                connection.execute(
                    parquet_copy_sql(
                        dynamic_sql,
                        dynamic_temp,
                        config.split.compression,
                        config.split.row_group_size,
                    )
                )
                connection.execute(
                    parquet_copy_sql(
                        static_sql,
                        static_temp,
                        config.split.compression,
                        config.split.row_group_size,
                    )
                )
                os.replace(dynamic_temp, dynamic_path)
                os.replace(static_temp, static_path)

                elapsed = time.perf_counter() - started
                manifest["files"][item.path] = {
                    "status": "complete",
                    "date": item.date,
                    "input_identity": item.identity,
                    "input_size_bytes": item.size_bytes,
                    "config_hash": stage_hash,
                    "dynamic_path": str(dynamic_path.resolve()),
                    "static_path": str(static_path.resolve()),
                    "parsed_rows": int(counts[0]),
                    "dynamic_message_rows": int(counts[1]),
                    "dynamic_valid_rows": dynamic_valid_rows,
                    "dynamic_invalid_rows": int(counts[1]) - dynamic_valid_rows,
                    "static_message_rows": int(counts[2]),
                    "static_valid_rows": static_valid_rows,
                    "static_invalid_rows": int(counts[2]) - static_valid_rows,
                    "other_rows": int(counts[3]),
                    "csv_reject_rows": csv_reject_rows,
                    "elapsed_seconds": round(elapsed, 3),
                    "completed_at_utc": datetime.now(timezone.utc).isoformat(),
                }
                write_json_atomic(manifest_path, manifest)
            except Exception as exc:
                dynamic_temp.unlink(missing_ok=True)
                static_temp.unlink(missing_ok=True)
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

            progress.update(item.size_bytes)
    finally:
        progress.close()
        connection.close()

    return manifest
