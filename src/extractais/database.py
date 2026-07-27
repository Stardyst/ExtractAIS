from __future__ import annotations

from pathlib import Path

import duckdb

from extractais.config import AppConfig
from extractais.storage import duckdb_temp_budget_bytes


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_database(
    config: AppConfig,
    output_reserve_bytes: int = 0,
) -> duckdb.DuckDBPyConnection:
    config.storage.temp_directory.mkdir(parents=True, exist_ok=True)
    temp_budget_bytes = duckdb_temp_budget_bytes(config, output_reserve_bytes)
    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET threads = {config.runtime.threads}")
    connection.execute(f"SET memory_limit = {sql_literal(config.runtime.memory_limit)}")
    connection.execute(
        f"SET temp_directory = {sql_literal(str(config.storage.temp_directory.resolve()))}"
    )
    connection.execute(
        f"SET max_temp_directory_size = {sql_literal(f'{temp_budget_bytes}B')}"
    )
    connection.execute(
        "SET partitioned_write_max_open_files = "
        f"{config.prepare.partition_write_max_open_files}"
    )
    connection.execute("SET preserve_insertion_order = false")
    if config.runtime.enable_progress:
        connection.execute("PRAGMA enable_progress_bar")
        connection.execute(
            f"SET progress_bar_time = {config.runtime.progress_bar_time_ms}"
        )
    else:
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
