from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict

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
from extractais.isolated import run_isolated
from extractais.sql import parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)
from extractais.stops import bucket_number, track_bucket_files


def _year_windows(config: AppConfig) -> str:
    rows = []
    for year in sorted(config.input.year_directories):
        rows.append(
            f"({year}, timestamp '{year}-01-01 00:00:00', timestamp '{year + 1}-01-01 00:00:00')"
        )
    return ", ".join(rows)


def _write_interval_bucket(
    connection,
    config: AppConfig,
    track_path: Path,
    candidates_path: Path,
    calls_path: Path,
    output: Path,
) -> int:
    tracks = parquet_sources([track_path])
    candidates = parquet_sources([candidates_path])
    calls = parquet_sources([calls_path])
    groups = parquet_sources(
        [config.storage.work_root / "stage05_ports" / "port_groups.parquet"]
    )
    gap_threshold = int(config.intervals.unknown_gap_hours * 3600)
    select_sql = f"""
        WITH valid_tracks AS (
            SELECT
                *,
                lag(timestamp_utc) OVER (
                    PARTITION BY mmsi ORDER BY point_seq
                ) AS observed_previous_time_utc
            FROM {tracks}
            WHERE NOT is_kinematic_outlier
        ),
        track_gaps AS (
            SELECT
                *,
                date_diff('second', observed_previous_time_utc, timestamp_utc)
                    AS observed_gap_seconds
            FROM valid_tracks
        ),
        calls_ordered AS (
            SELECT * FROM {calls} ORDER BY mmsi, entry_time_utc
        ),
        with_previous_call AS (
            SELECT
                t.*,
                p.port_call_id AS previous_call_id,
                p.port_group_id AS from_port_group_id,
                p.entry_time_utc AS previous_entry_time_utc,
                p.exit_time_utc AS previous_exit_time_utc
            FROM track_gaps t
            ASOF LEFT JOIN calls_ordered p
              ON t.mmsi = p.mmsi AND t.timestamp_utc >= p.entry_time_utc
        ),
        with_calls AS (
            SELECT
                t.*,
                n.port_call_id AS next_call_id,
                n.port_group_id AS to_port_group_id,
                n.entry_time_utc AS next_entry_time_utc,
                n.exit_time_utc AS next_exit_time_utc
            FROM with_previous_call t
            ASOF LEFT JOIN calls_ordered n
              ON t.mmsi = n.mmsi AND t.timestamp_utc < n.entry_time_utc
        ),
        candidate_context AS (
            SELECT
                t.*,
                previous_candidate.port_distance_km AS previous_port_distance_km,
                next_candidate.port_distance_km AS next_port_distance_km,
                first_candidate.port_distance_km AS first_candidate_distance_km,
                second_candidate.port_distance_km - first_candidate.port_distance_km
                    AS candidate_margin_km
            FROM with_calls t
            LEFT JOIN {candidates} previous_candidate
              ON t.mmsi = previous_candidate.mmsi
             AND t.point_seq = previous_candidate.point_seq
             AND t.from_port_group_id = previous_candidate.port_group_id
            LEFT JOIN {candidates} next_candidate
              ON t.mmsi = next_candidate.mmsi
             AND t.point_seq = next_candidate.point_seq
             AND t.to_port_group_id = next_candidate.port_group_id
            LEFT JOIN {candidates} first_candidate
              ON t.mmsi = first_candidate.mmsi
             AND t.point_seq = first_candidate.point_seq
             AND first_candidate.candidate_rank = 1
            LEFT JOIN {candidates} second_candidate
              ON t.mmsi = second_candidate.mmsi
             AND t.point_seq = second_candidate.point_seq
             AND second_candidate.candidate_rank = 2
        ),
        classified AS (
            SELECT
                *,
                CASE
                    WHEN previous_call_id IS NOT NULL
                     AND timestamp_utc <= previous_exit_time_utc
                    THEN 'IN_PORT'
                    WHEN previous_port_distance_km <= {config.ports.approach_radius_km}
                     AND next_port_distance_km <= {config.ports.approach_radius_km}
                    THEN CASE WHEN previous_port_distance_km <= next_port_distance_km
                              THEN 'DEPARTING' ELSE 'ARRIVING' END
                    WHEN previous_port_distance_km <= {config.ports.approach_radius_km}
                    THEN 'DEPARTING'
                    WHEN next_port_distance_km <= {config.ports.approach_radius_km}
                    THEN 'ARRIVING'
                    ELSE 'OCEAN'
                END AS state,
                candidate_margin_km < {config.ports.ambiguity_margin_km}
                    AS is_port_ambiguous
            FROM candidate_context
        ),
        marked AS (
            SELECT
                *,
                CASE
                    WHEN lag(state) OVER vessel_order IS NULL
                      OR state IS DISTINCT FROM lag(state) OVER vessel_order
                      OR from_port_group_id IS DISTINCT FROM lag(from_port_group_id) OVER vessel_order
                      OR to_port_group_id IS DISTINCT FROM lag(to_port_group_id) OVER vessel_order
                      OR observed_gap_seconds > {gap_threshold}
                    THEN 1 ELSE 0
                END AS new_segment
            FROM classified
            WINDOW vessel_order AS (PARTITION BY mmsi ORDER BY point_seq)
        ),
        grouped AS (
            SELECT
                *,
                sum(new_segment) OVER (
                    PARTITION BY mmsi ORDER BY point_seq ROWS UNBOUNDED PRECEDING
                ) AS segment_number
            FROM marked
        ),
        observed_segments AS (
            SELECT
                mmsi,
                min(timestamp_utc) AS start_time_utc,
                max(timestamp_utc) AS end_time_utc,
                state,
                from_port_group_id,
                to_port_group_id,
                count(*) AS point_count,
                max(observed_gap_seconds) FILTER (
                    WHERE observed_gap_seconds <= {gap_threshold}
                ) AS max_gap_seconds,
                bool_or(is_time_conflict) AS has_time_conflict,
                bool_or(coalesce(is_port_ambiguous, false)) AS has_port_ambiguity,
                false AS is_unknown_gap
            FROM grouped
            GROUP BY mmsi, segment_number, state, from_port_group_id, to_port_group_id
        ),
        gap_context AS (
            SELECT
                *,
                lag(from_port_group_id) OVER (
                    PARTITION BY mmsi ORDER BY point_seq
                ) AS before_gap_from_port_group_id
            FROM classified
        ),
        unknown_gaps AS (
            SELECT
                mmsi,
                observed_previous_time_utc AS start_time_utc,
                timestamp_utc AS end_time_utc,
                'UNKNOWN_GAP' AS state,
                before_gap_from_port_group_id AS from_port_group_id,
                CASE WHEN state = 'IN_PORT'
                     THEN from_port_group_id ELSE to_port_group_id END AS to_port_group_id,
                0::BIGINT AS point_count,
                observed_gap_seconds AS max_gap_seconds,
                false AS has_time_conflict,
                false AS has_port_ambiguity,
                true AS is_unknown_gap
            FROM gap_context
            WHERE observed_gap_seconds > {gap_threshold}
        ),
        all_segments AS (
            SELECT * FROM observed_segments
            UNION ALL
            SELECT * FROM unknown_gaps
        ),
        year_windows(year, year_start, year_end) AS (
            VALUES {_year_windows(config)}
        ),
        clipped AS (
            SELECT
                y.year,
                s.mmsi,
                greatest(s.start_time_utc, y.year_start) AS start_time_utc,
                least(s.end_time_utc, y.year_end) AS end_time_utc,
                s.state,
                s.from_port_group_id,
                s.to_port_group_id,
                s.point_count,
                s.max_gap_seconds,
                CASE
                    WHEN s.is_unknown_gap THEN 'NO_AIS'
                    WHEN s.has_port_ambiguity THEN 'PORT_AMBIGUOUS'
                    WHEN s.has_time_conflict THEN 'TIME_CONFLICT'
                    ELSE 'OBSERVED'
                END AS quality_flag
            FROM all_segments s
            JOIN year_windows y
              ON s.end_time_utc >= y.year_start AND s.start_time_utc < y.year_end
        ),
        named AS (
            SELECT
                c.*,
                source_group.port_group_name AS from_port_group_name,
                target_group.port_group_name AS to_port_group_name
            FROM clipped c
            LEFT JOIN {groups} source_group
              ON c.from_port_group_id = source_group.port_group_id
            LEFT JOIN {groups} target_group
              ON c.to_port_group_id = target_group.port_group_id
        )
        SELECT
            year,
            mmsi,
            concat(
                year::VARCHAR, '-', mmsi::VARCHAR, '-',
                lpad(row_number() OVER (
                    PARTITION BY year, mmsi ORDER BY start_time_utc, state
                )::VARCHAR, 6, '0')
            ) AS segment_id,
            start_time_utc,
            end_time_utc,
            state,
            from_port_group_id,
            from_port_group_name,
            to_port_group_id,
            to_port_group_name,
            point_count,
            max_gap_seconds,
            quality_flag,
            {bucket_number(track_path)}::INTEGER AS mmsi_bucket
        FROM named
        WHERE end_time_utc >= start_time_utc
        ORDER BY mmsi, start_time_utc, state
    """
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


def _write_interval_bucket_worker(
    config: AppConfig,
    track_path: Path,
    candidates_path: Path,
    calls_path: Path,
    output: Path,
) -> int:
    connection = open_database(config)
    try:
        return _write_interval_bucket(
            connection,
            config,
            track_path,
            candidates_path,
            calls_path,
            output,
        )
    finally:
        connection.close()


def _export_yearly(config: AppConfig, sources: list[Path], output_root: Path) -> int:
    temporary = temporary_directory(output_root)
    remove_path(temporary, config.storage.work_root)
    temporary.parent.mkdir(parents=True, exist_ok=True)
    source = parquet_sources(sources)
    connection = open_database(config)
    try:
        exported_count = int(
            connection.execute(f"""
                COPY (
                    SELECT * EXCLUDE (mmsi_bucket), mmsi_bucket
                    FROM {source}
                    ORDER BY year, mmsi, start_time_utc
                )
                TO {sql_literal(str(temporary.resolve()))}
                (
                    FORMAT PARQUET,
                    PARTITION_BY (year),
                    COMPRESSION {config.prepare.compression.upper()},
                    ROW_GROUP_SIZE {config.prepare.row_group_size}
                )
            """).fetchone()[0]
        )
    finally:
        connection.close()
    replace_directory(temporary, output_root, config.storage.work_root)
    return exported_count


def build_intervals(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    tracks = track_bucket_files(config)
    calls_root = config.storage.work_root / "stage06_port_calls"
    groups_path = config.storage.work_root / "stage05_ports" / "port_groups.parquet"
    manifest_path = config.storage.work_root / "manifests" / "intervals.json"
    manifest = load_stage_manifest(manifest_path, "intervals")
    stage_hash = config.stage_hash("intervals", "ports", "prepare")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
    segment_root = config.storage.work_root / "stage07_intervals"

    progress = tqdm(
        tracks,
        desc="build state intervals",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
    )
    outputs: list[Path] = []
    for track_path in progress:
        bucket = bucket_number(track_path)
        calls_bucket = calls_root / f"mmsi_bucket={bucket:04d}"
        candidates_path = calls_bucket / "candidates.parquet"
        calls_path = calls_bucket / "port_calls.parquet"
        dependencies = [track_path, candidates_path, calls_path, groups_path]
        if not all(path.exists() for path in dependencies):
            raise RuntimeError(f"Port-call outputs are incomplete for bucket {bucket:04d}")
        source_signature = signature(
            (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
            for path in dependencies
        )
        output = segment_root / f"mmsi_bucket={bucket:04d}" / "part.parquet"
        outputs.append(output)
        key = f"bucket:{bucket:04d}"
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, [output]
        ):
            continue
        started = time.perf_counter()
        temporary = temporary_file(output)
        temporary.unlink(missing_ok=True)
        output.parent.mkdir(parents=True, exist_ok=True)
        worker = run_isolated(
            _write_interval_bucket_worker,
            config,
            track_path,
            candidates_path,
            calls_path,
            temporary,
        )
        replace_file(temporary, output)
        manifest["items"][key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": source_signature,
            "output": str(output.resolve()),
            "interval_count": worker.value,
            "worker_process_id": worker.process_id,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)

    output_root = config.storage.work_root / "outputs" / "trajectory_intervals"
    export_signature = signature(
        (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
        for path in outputs
    )
    if force or not item_is_complete(
        manifest, "yearly_export", stage_hash, export_signature, [output_root]
    ):
        started = time.perf_counter()
        worker = run_isolated(_export_yearly, config, outputs, output_root)
        manifest["items"]["yearly_export"] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": export_signature,
            "output": str(output_root.resolve()),
            "interval_count": worker.value,
            "worker_process_id": worker.process_id,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
    return manifest
