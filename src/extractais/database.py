from __future__ import annotations

from pathlib import Path

import duckdb

from extractais.config import AppConfig


def sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def open_database(
    config: AppConfig,
    temp_directory: Path,
    *,
    worker: bool,
) -> duckdb.DuckDBPyConnection:
    temp_directory.mkdir(parents=True, exist_ok=True)
    threads = config.runtime.worker_threads if worker else config.runtime.global_threads
    memory = config.runtime.worker_memory if worker else config.runtime.global_memory
    temp_limit = (
        config.runtime.worker_temp_gib
        if worker
        else config.runtime.worker_temp_gib * max(1, len(config.storage.track_roots))
    )
    connection = duckdb.connect(database=":memory:")
    connection.execute(f"SET threads={threads}")
    connection.execute(f"SET memory_limit={sql_literal(memory)}")
    connection.execute(
        f"SET temp_directory={sql_literal(str(temp_directory.resolve()))}"
    )
    connection.execute(f"SET max_temp_directory_size={sql_literal(f'{temp_limit}GiB')}")
    connection.execute("SET preserve_insertion_order=false")
    connection.execute("PRAGMA disable_progress_bar")
    return connection


def parquet_copy_sql(
    select_sql: str,
    output_path: Path,
    config: AppConfig,
    *,
    order_by: str | None = None,
) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    ordered = f"SELECT * FROM ({select_sql}) AS result ORDER BY {order_by}" if order_by else select_sql
    return f"""
        COPY ({ordered})
        TO {sql_literal(str(output_path.resolve()))}
        (
            FORMAT PARQUET,
            COMPRESSION {config.layout.compression.upper()},
            ROW_GROUP_SIZE {config.layout.row_group_size}
        )
    """
