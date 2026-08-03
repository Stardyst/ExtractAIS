from __future__ import annotations

import json
import shutil
from pathlib import Path
from typing import Any

from extractais import __version__
from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.intervals import interval_path
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature, write_json_atomic
from extractais.sql import parquet_sources
from extractais.storage import (
    ensure_space,
    evidence_root_for_partition,
    lane_for_partition,
    port_call_path,
    port_context_path,
    shared_output_requirement,
    shared_temp_requirement,
    total_file_size,
)


def _partial_paths(config: AppConfig, partition: int) -> tuple[Path, Path, Path]:
    root = config.storage.products_root / "validation" / "partials"
    return (
        root / "states" / f"partition={partition:04d}.parquet",
        root / "ports" / f"partition={partition:04d}.parquet",
        root / "ambiguous" / f"partition={partition:04d}.parquet",
    )


def _validation_partial_worker(
    config: AppConfig,
    partition: int,
    outputs: tuple[Path, Path, Path],
    heartbeat_path: Path,
) -> dict[str, int]:
    temporary_outputs = tuple(path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs)
    worker_temp = config.storage.temp_root / f"validation-{partition:04d}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    intervals = [
        interval_path(config, year, partition)
        for year in sorted(config.input.year_directories)
    ]
    calls = port_call_path(config, partition)
    context = port_context_path(config, partition)
    source_bytes = total_file_size([*intervals, calls, context])
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        shared_output_requirement(config, max(source_bytes, 1024**2)),
        f"validation partial {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"validation temporary {partition:04d}",
    )
    connection = open_database(config, worker_temp, worker=True)
    try:
        heartbeat(heartbeat_path, "summarizing interval states")
        state_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT
                        year, state, quality_flag,
                        count(*) AS interval_count,
                        sum(ais_point_count) AS ais_point_count,
                        count(DISTINCT mmsi) AS vessel_count
                    FROM {parquet_sources(intervals)}
                    GROUP BY year, state, quality_flag
                    """,
                    temporary_outputs[0],
                    config,
                    order_by="year, state, quality_flag",
                )
            ).fetchone()[0]
        )
        heartbeat(heartbeat_path, "summarizing port calls")
        port_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT
                        port_group_id,
                        arg_min(port_group_name, entry_time_utc) AS port_group_name,
                        arg_min(port_country_or_area, entry_time_utc)
                            AS country_or_area_name,
                        count(*) AS port_call_count,
                        count(DISTINCT mmsi) AS vessel_count,
                        count(*) FILTER (WHERE has_port_ambiguity)
                            AS ambiguous_call_count,
                        min(minimum_ambiguity_margin_km)
                            AS minimum_ambiguity_margin_km
                    FROM {parquet_sources([calls])}
                    GROUP BY port_group_id
                    """,
                    temporary_outputs[1],
                    config,
                    order_by="port_group_id",
                )
            ).fetchone()[0]
        )
        heartbeat(heartbeat_path, "writing ambiguous point evidence")
        ambiguous_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT
                        x.mmsi, x.point_seq, x.timestamp_utc,
                        x.latitude, x.longitude,
                        x.port_group_id, x.port_group_name,
                        x.second_port_group_id, x.second_port_group_name,
                        x.port_distance_km, x.second_port_distance_km,
                        x.ambiguity_margin_km, x.track_partition_id
                    FROM {parquet_sources([context])} x
                    WHERE x.ambiguity_margin_km < {config.ports.ambiguity_margin_km}
                    """,
                    temporary_outputs[2],
                    config,
                    order_by="mmsi, point_seq",
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)
    for temporary, output in zip(temporary_outputs, outputs):
        atomic_replace(temporary, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": state_count + port_count + ambiguous_count,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def _merge_validation_worker(
    config: AppConfig,
    state_paths: list[Path],
    port_paths: list[Path],
    ambiguous_paths: list[Path],
    outputs: tuple[Path, Path, Path, Path],
    heartbeat_path: Path,
) -> dict[str, int]:
    state_csv, port_csv, harbor_csv, ambiguous_output = outputs
    temporary = tuple(path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs)
    worker_temp = config.storage.temp_root / "validation-merge"
    source_bytes = total_file_size([*state_paths, *port_paths, *ambiguous_paths])
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        max(source_bytes, 1024**2),
        "validation reports",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        "validation merge temporary data",
    )
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(config, worker_temp, worker=False)
    try:
        heartbeat(heartbeat_path, "merging state summaries")
        connection.execute(
            f"""
            COPY (
                SELECT
                    year, state, quality_flag,
                    sum(interval_count) AS interval_count,
                    sum(ais_point_count) AS ais_point_count,
                    sum(vessel_count) AS vessel_count
                FROM {parquet_sources(state_paths)}
                GROUP BY year, state, quality_flag
                ORDER BY year, state, quality_flag
            ) TO {sql_literal(str(temporary[0].resolve()))} (HEADER, DELIMITER ',')
            """
        )
        heartbeat(heartbeat_path, "merging port quality")
        coverage = parquet_sources([config.storage.products_root / "ports" / "port_coverage.parquet"])
        connection.execute(
            f"""
            CREATE TEMP TABLE port_quality AS
            WITH calls AS (
                SELECT
                    port_group_id,
                    sum(port_call_count) AS port_call_count,
                    sum(vessel_count) AS vessel_count,
                    sum(ambiguous_call_count) AS ambiguous_call_count,
                    min(minimum_ambiguity_margin_km) AS minimum_ambiguity_margin_km
                FROM {parquet_sources(port_paths)}
                GROUP BY port_group_id
            )
            SELECT
                c.*,
                coalesce(p.port_call_count, 0) AS port_call_count,
                coalesce(p.vessel_count, 0) AS calling_vessel_count,
                coalesce(p.ambiguous_call_count, 0) AS ambiguous_call_count,
                p.minimum_ambiguity_margin_km,
                CASE WHEN coalesce(p.port_call_count, 0) > 0
                     THEN p.ambiguous_call_count::DOUBLE / p.port_call_count END
                    AS ambiguity_rate
            FROM {coverage} c
            LEFT JOIN calls p USING (port_group_id)
            """
        )
        connection.execute(
            f"COPY (SELECT * FROM port_quality ORDER BY port_group_id) "
            f"TO {sql_literal(str(temporary[1].resolve()))} (HEADER, DELIMITER ',')"
        )
        connection.execute(
            f"""
            COPY (
                SELECT
                    harbor_size,
                    count(*) AS port_group_count,
                    count(*) FILTER (WHERE anchor_count > 0) AS recognized_group_count,
                    sum(port_call_count) AS port_call_count,
                    sum(calling_vessel_count) AS calling_vessel_count,
                    sum(ambiguous_call_count) AS ambiguous_call_count
                FROM port_quality
                GROUP BY harbor_size
                ORDER BY harbor_size
            ) TO {sql_literal(str(temporary[2].resolve()))} (HEADER, DELIMITER ',')
            """
        )
        ambiguous_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"SELECT * FROM {parquet_sources(ambiguous_paths)}",
                    temporary[3],
                    config,
                    order_by="track_partition_id, mmsi, point_seq",
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)
    for source, output in zip(temporary, outputs):
        atomic_replace(source, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": ambiguous_count,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def build_validation(config: AppConfig, store: CheckpointStore) -> None:
    tasks: list[StageTask] = []
    stage_hash = signature([config.raw["ports"], config.raw["intervals"]])
    for partition in range(config.layout.track_partitions):
        dependencies = [
            port_call_path(config, partition),
            port_context_path(config, partition),
            *[
                interval_path(config, year, partition)
                for year in sorted(config.input.year_directories)
            ],
        ]
        outputs = _partial_paths(config, partition)
        task_signature = signature(
            [stage_hash, [(path.stat().st_size, path.stat().st_mtime_ns) for path in dependencies]]
        )
        beat = config.storage.temp_root / "heartbeats" / f"validation-{partition:04d}.json"
        tasks.append(
            StageTask(
                key=f"{partition:04d}", signature=task_signature,
                source_bytes=max(1, sum(path.stat().st_size for path in dependencies)),
                outputs=outputs, heartbeat_path=beat,
                call=IsolatedCall(
                    key=f"{partition:04d}", target=_validation_partial_worker,
                    args=(config, partition, outputs, beat),
                    resource=(
                        f"{lane_for_partition(config, partition)}|"
                        f"{evidence_root_for_partition(config, partition)}"
                    ),
                ),
            )
        )
    run_stage_tasks(
        config, store, "validation_partials", tasks,
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )

    state_paths, port_paths, ambiguous_paths = zip(
        *[_partial_paths(config, partition) for partition in range(config.layout.track_partitions)]
    )
    validation_root = config.storage.products_root / "validation"
    outputs = (
        validation_root / "state_summary.csv",
        validation_root / "port_quality.csv",
        validation_root / "harbor_size_quality.csv",
        validation_root / "ambiguous_port_points.parquet",
    )
    merge_signature = signature(
        [
            stage_hash,
            [(path.stat().st_size, path.stat().st_mtime_ns) for path in (*state_paths, *port_paths, *ambiguous_paths)],
        ]
    )
    merge_beat = config.storage.temp_root / "heartbeats" / "validation-merge.json"
    run_stage_tasks(
        config,
        store,
        "validation",
        [
            StageTask(
                key="global",
                signature=merge_signature,
                source_bytes=max(
                    1,
                    sum(
                        path.stat().st_size
                        for path in (*state_paths, *port_paths, *ambiguous_paths)
                    ),
                ),
                outputs=outputs,
                heartbeat_path=merge_beat,
                call=IsolatedCall(
                    key="global",
                    target=_merge_validation_worker,
                    args=(
                        config,
                        list(state_paths),
                        list(port_paths),
                        list(ambiguous_paths),
                        outputs,
                        merge_beat,
                    ),
                    resource="validation-products",
                ),
            )
        ],
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )

    summary = {
        "pipeline_version": __version__,
        "track_partitions": config.layout.track_partitions,
        "years": sorted(config.input.year_directories),
        "trajectory_intervals": str((config.storage.products_root / "trajectory_intervals").resolve()),
        "port_quality": str((validation_root / "port_quality.csv").resolve()),
        "country_fields": [
            "from_port_country_or_area",
            "to_port_country_or_area",
        ],
    }
    write_json_atomic(validation_root / "summary.json", summary)
