from __future__ import annotations

import os
import time
from pathlib import Path
from typing import Any, Dict

from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.fileutils import remove_path, replace_directory, temporary_directory
from extractais.gitmeta import git_commit
from extractais.isolated import run_isolated
from extractais.manifest import write_json_atomic
from extractais.sql import parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)


def _csv_copy(connection, select_sql: str, output: Path) -> None:
    connection.execute(f"""
        COPY ({select_sql}) TO {sql_literal(str(output.resolve()))}
        (FORMAT CSV, HEADER true)
    """)


def _build_validation_in_process(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    calls = sorted(
        (config.storage.work_root / "stage06_port_calls").glob(
            "mmsi_bucket=*/port_calls.parquet"
        )
    )
    candidates = sorted(
        (config.storage.work_root / "stage06_port_calls").glob(
            "mmsi_bucket=*/candidates.parquet"
        )
    )
    intervals = sorted(
        (config.storage.work_root / "stage07_intervals").glob(
            "mmsi_bucket=*/part.parquet"
        )
    )
    ports_root = config.storage.work_root / "stage05_ports"
    port_dependencies = [
        ports_root / "port_groups.parquet",
        ports_root / "port_coverage.parquet",
        ports_root / "unmatched_anchor_candidates.parquet",
    ]
    dependencies = calls + candidates + intervals + port_dependencies
    if not calls or not candidates or not intervals or not all(path.exists() for path in port_dependencies):
        raise RuntimeError("Final-stage artifacts are incomplete; run `extractais intervals` first")

    source_signature = signature(
        (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
        for path in dependencies
    )
    stage_hash = config.stage_hash("ports", "intervals")
    manifest_path = config.storage.work_root / "manifests" / "validate.json"
    manifest = load_stage_manifest(manifest_path, "validate")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
    output_root = config.storage.work_root / "outputs" / "validation"
    required = [
        output_root / "port_quality.parquet",
        output_root / "port_quality.csv",
        output_root / "harbor_size_quality.csv",
        output_root / "state_summary.csv",
        output_root / "ambiguous_port_points.parquet",
        output_root / "summary.json",
    ]
    if not force and item_is_complete(
        manifest, "reports", stage_hash, source_signature, required
    ):
        return manifest

    started = time.perf_counter()
    temporary = temporary_directory(output_root)
    remove_path(temporary, config.storage.work_root)
    temporary.mkdir(parents=True, exist_ok=True)
    connection = open_database(config)
    try:
        calls_source = parquet_sources(calls)
        candidates_source = parquet_sources(candidates)
        intervals_source = parquet_sources(intervals)
        groups_source = parquet_sources([ports_root / "port_groups.parquet"])
        coverage_source = parquet_sources([ports_root / "port_coverage.parquet"])
        unmatched_source = parquet_sources(
            [ports_root / "unmatched_anchor_candidates.parquet"]
        )
        connection.execute(f"CREATE TEMP VIEW all_calls AS SELECT * FROM {calls_source}")
        connection.execute(
            f"CREATE TEMP VIEW all_candidates AS SELECT * FROM {candidates_source}"
        )
        connection.execute(
            f"CREATE TEMP VIEW all_intervals AS SELECT * FROM {intervals_source}"
        )
        port_quality_sql = f"""
            WITH call_metrics AS (
                SELECT
                    port_group_id,
                    count(*) AS port_call_count,
                    count(DISTINCT mmsi) AS calling_vessel_count,
                    median(minimum_port_distance_km) AS median_minimum_distance_km,
                    avg((matched_stop_count > 0)::INTEGER) AS stop_confirmed_fraction,
                    avg((source_at_dock_points > 0)::INTEGER) AS source_at_dock_fraction,
                    avg((source_matched_port_points > 0)::INTEGER)
                        AS source_matched_port_fraction,
                    avg((minimum_ambiguity_margin_km < {config.ports.ambiguity_margin_km})::INTEGER)
                        AS ambiguous_call_fraction
                FROM all_calls
                GROUP BY port_group_id
            )
            SELECT
                g.port_group_id, g.port_group_name, g.harbor_size,
                g.member_port_count, c.anchor_count, c.supporting_stop_events,
                c.anchor_vessel_support, c.source_at_dock_points,
                coalesce(m.port_call_count, 0) AS port_call_count,
                coalesce(m.calling_vessel_count, 0) AS calling_vessel_count,
                m.median_minimum_distance_km, m.stop_confirmed_fraction,
                m.source_at_dock_fraction, m.source_matched_port_fraction,
                m.ambiguous_call_fraction,
                CASE
                    WHEN c.anchor_count = 0 THEN 'NO_ANCHOR'
                    WHEN coalesce(m.port_call_count, 0) = 0 THEN 'NO_CALL'
                    WHEN coalesce(m.ambiguous_call_fraction, 0) >= 0.25 THEN 'HIGH_AMBIGUITY'
                    ELSE 'REVIEWABLE'
                END AS review_status
            FROM {groups_source} g
            LEFT JOIN {coverage_source} c USING (port_group_id)
            LEFT JOIN call_metrics m USING (port_group_id)
            ORDER BY review_status, port_call_count DESC, port_group_id
        """
        connection.execute(
            parquet_copy_sql(
                port_quality_sql,
                temporary / "port_quality.parquet",
                config.prepare.compression,
                config.prepare.row_group_size,
            )
        )
        _csv_copy(connection, port_quality_sql, temporary / "port_quality.csv")

        harbor_sql = f"""
            SELECT
                harbor_size,
                count(*) AS port_group_count,
                count(*) FILTER (WHERE anchor_count > 0) AS groups_with_anchor,
                sum(port_call_count) AS port_call_count,
                sum(calling_vessel_count) AS summed_calling_vessels,
                avg(ambiguous_call_fraction) AS mean_ambiguous_call_fraction,
                avg(stop_confirmed_fraction) AS mean_stop_confirmed_fraction
            FROM ({port_quality_sql})
            GROUP BY harbor_size
            ORDER BY CASE harbor_size
                WHEN 'Large' THEN 1 WHEN 'Medium' THEN 2 WHEN 'Small' THEN 3
                WHEN 'Very Small' THEN 4 ELSE 5 END
        """
        _csv_copy(connection, harbor_sql, temporary / "harbor_size_quality.csv")

        state_sql = """
            SELECT
                year, state, quality_flag,
                count(*) AS interval_count,
                count(DISTINCT mmsi) AS vessel_count,
                sum(point_count) AS point_count
            FROM all_intervals
            GROUP BY year, state, quality_flag
            ORDER BY year, state, quality_flag
        """
        _csv_copy(connection, state_sql, temporary / "state_summary.csv")

        ambiguity_sql = f"""
            SELECT
                first.mmsi, first.timestamp_utc, first.latitude, first.longitude,
                first.port_group_id AS first_port_group_id,
                first.port_group_name AS first_port_group_name,
                first.port_distance_km AS first_distance_km,
                second.port_group_id AS second_port_group_id,
                second.port_group_name AS second_port_group_name,
                second.port_distance_km AS second_distance_km,
                second.port_distance_km - first.port_distance_km AS margin_km
            FROM all_candidates first
            JOIN all_candidates second
              ON first.mmsi = second.mmsi
             AND first.point_seq = second.point_seq
             AND first.candidate_rank = 1
             AND second.candidate_rank = 2
            WHERE second.port_distance_km - first.port_distance_km
                  < {config.ports.ambiguity_margin_km}
            ORDER BY margin_km, first.mmsi, first.timestamp_utc
            LIMIT 100000
        """
        connection.execute(
            parquet_copy_sql(
                ambiguity_sql,
                temporary / "ambiguous_port_points.parquet",
                config.prepare.compression,
                config.prepare.row_group_size,
            )
        )

        summary = {
            "generated_at_utc": utc_now(),
            "config_hash": stage_hash,
            "git_commit": git_commit(project_root),
            "port_groups": int(
                connection.execute(f"SELECT count(*) FROM {groups_source}").fetchone()[0]
            ),
            "port_groups_with_anchors": int(
                connection.execute(
                    f"SELECT count(*) FROM {coverage_source} WHERE anchor_count > 0"
                ).fetchone()[0]
            ),
            "unmatched_anchor_candidates": int(
                connection.execute(f"SELECT count(*) FROM {unmatched_source}").fetchone()[0]
            ),
            "port_calls": int(connection.execute("SELECT count(*) FROM all_calls").fetchone()[0]),
            "trajectory_intervals": int(
                connection.execute("SELECT count(*) FROM all_intervals").fetchone()[0]
            ),
            "unknown_gap_intervals": int(
                connection.execute(
                    "SELECT count(*) FROM all_intervals WHERE state = 'UNKNOWN_GAP'"
                ).fetchone()[0]
            ),
        }
        write_json_atomic(temporary / "summary.json", summary)
    finally:
        connection.close()
    replace_directory(temporary, output_root, config.storage.work_root)
    manifest["items"]["reports"] = {
        "status": "complete",
        "config_hash": stage_hash,
        "source_signature": source_signature,
        "output": str(output_root.resolve()),
        "worker_process_id": os.getpid(),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "completed_at_utc": utc_now(),
    }
    save_stage_manifest(manifest_path, manifest)
    return manifest


def build_validation(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    return run_isolated(
        _build_validation_in_process,
        config,
        project_root,
        force,
    ).value
