from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.inventory import Inventory
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.schema import DYNAMIC_SELECT, STATIC_SELECT, normalized_day_sql, sql_int_list
from extractais.storage import directory_size, ensure_space


def dynamic_run_path(config: AppConfig, lane: int, date: str) -> Path:
    return config.storage.track_roots[lane] / "ingest_runs" / "dynamic" / f"date={date}.parquet"


def static_run_path(config: AppConfig, date: str) -> Path:
    return config.storage.products_root / "ingest_runs" / "static" / f"date={date}.parquet"


def stats_run_path(config: AppConfig, date: str) -> Path:
    return config.storage.products_root / "ingest_runs" / "stats" / f"date={date}.parquet"


def _temporary(path: Path) -> Path:
    return path.with_name(path.stem + ".tmp" + path.suffix)


def _ingest_worker(
    config: AppConfig,
    source: Path,
    date: str,
    outputs: tuple[Path, ...],
    heartbeat_path: Path,
    safe_mode: bool = False,
) -> dict[str, int]:
    lane_count = len(config.storage.track_roots)
    dynamic_outputs = outputs[:lane_count]
    static_output = outputs[lane_count]
    stats_output = outputs[lane_count + 1]
    temporary_outputs = tuple(_temporary(path) for path in outputs)
    worker_temp = config.storage.temp_root / f"ingest-{date}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)

    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        min(int(config.runtime.worker_temp_gib * 1024**3), source.stat().st_size * 6),
        f"ingest {date} temporary data",
    )
    for root in config.storage.track_roots:
        ensure_space(
            root,
            config.storage.reserves_gib["tracks"],
            source.stat().st_size,
            f"ingest {date} track output",
        )
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        source.stat().st_size,
        f"ingest {date} static output",
    )

    connection = open_database(
        config,
        worker_temp,
        worker=True,
        threads_override=1 if safe_mode else None,
    )
    try:
        heartbeat(
            heartbeat_path,
            "parsing raw CSV (safe single-thread reader)"
            if safe_mode
            else "parsing raw CSV",
        )
        connection.execute(
            f"CREATE TEMP TABLE normalized_day AS "
            f"{normalized_day_sql(source, safe_mode=safe_mode)}"
        )
        dynamic_sql = DYNAMIC_SELECT.format(
            dynamic_types=sql_int_list(list(config.cleaning.dynamic_message_types))
        )
        static_sql = STATIC_SELECT.format(
            static_types=sql_int_list(list(config.cleaning.static_message_types))
        )
        partition_expression = (
            f"cast((((mmsi * 1103515245 + 12345) % 2147483647) "
            f"& {config.layout.track_partitions - 1}) AS INTEGER)"
        )
        connection.execute(
            f"""
            CREATE TEMP VIEW dynamic_partitioned AS
            SELECT *, {partition_expression} AS track_partition_id
            FROM ({dynamic_sql})
            """
        )
        connection.execute(
            f"""
            CREATE TEMP VIEW static_partitioned AS
            SELECT *, {partition_expression} AS track_partition_id
            FROM ({static_sql})
            """
        )

        dynamic_rows = 0
        for lane, output in enumerate(temporary_outputs[:lane_count]):
            heartbeat(heartbeat_path, f"writing dynamic lane {lane + 1}/{lane_count}")
            result = connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT * FROM dynamic_partitioned
                    WHERE track_partition_id % {lane_count} = {lane}
                    """,
                    output,
                    config,
                    order_by="track_partition_id, mmsi, timestamp_utc, latitude, longitude",
                )
            ).fetchone()
            dynamic_rows += int(result[0])

        heartbeat(heartbeat_path, "writing static messages")
        static_rows = int(
            connection.execute(
                parquet_copy_sql(
                    "SELECT * FROM static_partitioned",
                    temporary_outputs[lane_count],
                    config,
                    order_by="track_partition_id, mmsi, timestamp_utc",
                )
            ).fetchone()[0]
        )
        heartbeat(heartbeat_path, "writing partition statistics")
        connection.execute(
            parquet_copy_sql(
                """
                SELECT
                    track_partition_id,
                    count(*)::BIGINT AS row_count,
                    min(timestamp_utc) AS first_time_utc,
                    max(timestamp_utc) AS last_time_utc
                FROM dynamic_partitioned
                GROUP BY track_partition_id
                """,
                temporary_outputs[lane_count + 1],
                config,
                order_by="track_partition_id",
            )
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)

    heartbeat(heartbeat_path, "committing")
    for temporary, output in zip(temporary_outputs, outputs):
        atomic_replace(temporary, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": dynamic_rows + static_rows,
        "dynamic_rows": dynamic_rows,
        "static_rows": static_rows,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def ingest(config: AppConfig, inventory: Inventory, store: CheckpointStore) -> None:
    tasks: list[StageTask] = []
    lane_count = len(config.storage.track_roots)
    stage_hash = config.section_hash("layout", "cleaning")
    for item in inventory.files:
        source = Path(item.path)
        outputs = tuple(
            dynamic_run_path(config, lane, item.date) for lane in range(lane_count)
        ) + (static_run_path(config, item.date), stats_run_path(config, item.date))
        task_signature = signature([stage_hash, item.identity])
        heartbeat_path = config.storage.temp_root / "heartbeats" / f"ingest-{item.date}.json"
        tasks.append(
            StageTask(
                key=item.date,
                signature=task_signature,
                source_bytes=item.size_bytes,
                outputs=outputs,
                heartbeat_path=heartbeat_path,
                call=IsolatedCall(
                    key=item.date,
                    target=_ingest_worker,
                    args=(config, source, item.date, outputs, heartbeat_path, False),
                    resource="raw-input",
                    fallback_args=(
                        config,
                        source,
                        item.date,
                        outputs,
                        heartbeat_path,
                        True,
                    ),
                    fallback_description="safe single-thread CSV reader",
                ),
            )
        )

    def complete(task: StageTask, value: Any, _pid: int, _elapsed: float) -> tuple[int, int]:
        return int(value["output_bytes"]), int(value["row_count"])

    run_stage_tasks(config, store, "ingest", tasks, complete)


def ingest_run_size(config: AppConfig) -> int:
    roots = [root / "ingest_runs" for root in config.storage.track_roots]
    roots.append(config.storage.products_root / "ingest_runs")
    return sum(directory_size(root) for root in roots)
