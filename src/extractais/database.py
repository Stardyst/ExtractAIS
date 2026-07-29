from __future__ import annotations

import os
from pathlib import Path

import duckdb

from extractais.config import AppConfig
from extractais.storage import GIB, duckdb_temp_budget_bytes


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_database(
    config: AppConfig,
    output_reserve_bytes: int = 0,
    workload: str = "global",
    worker_temp_directory: Path | None = None,
    threads_override: int | None = None,
) -> duckdb.DuckDBPyConnection:
    if workload not in {"global", "bucket"}:
        raise ValueError(f"Unknown DuckDB workload profile: {workload}")
    threads = (
        config.runtime.bucket_threads
        if workload == "bucket"
        else config.runtime.threads
    )
    if threads_override is not None:
        if threads_override <= 0:
            raise ValueError("threads_override must be positive")
        threads = min(threads, threads_override)
    memory_limit = (
        config.runtime.bucket_memory_limit
        if workload == "bucket"
        else config.runtime.memory_limit
    )
    worker_temp = worker_temp_directory or (
        config.storage.temp_directory / f"worker-{os.getpid()}"
    )
    worker_temp.mkdir(parents=True, exist_ok=True)
    temp_budget_bytes = duckdb_temp_budget_bytes(config, output_reserve_bytes)
    if workload == "bucket":
        temp_budget_bytes = min(
            temp_budget_bytes,
            int(config.runtime.bucket_temp_limit_gb * GIB),
        )
    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET threads = {threads}")
    connection.execute(f"SET memory_limit = {sql_literal(memory_limit)}")
    connection.execute(
        f"SET temp_directory = {sql_literal(str(worker_temp.resolve()))}"
    )
    connection.execute(
        f"SET max_temp_directory_size = {sql_literal(f'{temp_budget_bytes}B')}"
    )
    connection.execute(
        "SET partitioned_write_max_open_files = "
        f"{config.prepare.partition_write_max_open_files}"
    )
    connection.execute("SET preserve_insertion_order = false")
    # DuckDB's operator ETA omits blocking finalization and buffered flushes.
    # The parent process reports ETA from completed work-unit throughput instead.
    connection.execute("PRAGMA disable_progress_bar")
    return connection


def parquet_copy_sql(
    select_sql: str,
    output_path: Path,
    compression: str,
    row_group_size: int,
) -> str:
    return f"""
COPY ({select_sql})
TO {sql_literal(str(output_path.resolve()))}
(FORMAT PARQUET, COMPRESSION {compression.upper()}, ROW_GROUP_SIZE {row_group_size})
"""
