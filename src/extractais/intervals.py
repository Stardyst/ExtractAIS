from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from extractais.bucketstage import BucketExecution, BucketTask, run_bucket_stage
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.fileutils import (
    remove_path,
    replace_file,
)
from extractais.gitmeta import git_commit
from extractais.isolated import IsolatedCall
from extractais.sql import parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)
from extractais.storage import free_space_bytes, total_file_size
from extractais.stops import bucket_number, track_bucket_files


def _year_windows(config: AppConfig) -> str:
    rows = []
    for year in sorted(config.input.year_directories):
        rows.append(
            f"({year}, timestamp '{year}-01-01 00:00:00', timestamp '{year + 1}-01-01 00:00:00')"
        )
    return ", ".join(rows)


def _interval_select_sql(
    config: AppConfig,
    track_path: Path,
    candidates_path: Path,
    context_path: Path,
    calls_path: Path,
) -> str:
    tracks = parquet_sources([track_path])
    candidates = parquet_sources([candidates_path])
    context = parquet_sources([context_path])
    calls = parquet_sources([calls_path])
    groups = parquet_sources(
        [config.storage.work_root / "stage05_ports" / "port_groups.parquet"]
    )
    gap_threshold = int(config.intervals.unknown_gap_hours * 3600)
    return f"""
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
                first_context.port_distance_km AS first_candidate_distance_km,
                first_context.ambiguity_margin_km AS candidate_margin_km
            FROM with_calls t
            LEFT JOIN {candidates} previous_candidate
              ON t.mmsi = previous_candidate.mmsi
             AND t.point_seq = previous_candidate.point_seq
             AND t.from_port_group_id = previous_candidate.port_group_id
            LEFT JOIN {candidates} next_candidate
              ON t.mmsi = next_candidate.mmsi
             AND t.point_seq = next_candidate.point_seq
             AND t.to_port_group_id = next_candidate.port_group_id
            LEFT JOIN {context} first_context
              ON t.mmsi = first_context.mmsi
             AND t.point_seq = first_context.point_seq
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
    """


def _write_interval_bucket_worker(
    config: AppConfig,
    track_path: Path,
    candidates_path: Path,
    context_path: Path,
    calls_path: Path,
    temporary_root: Path,
) -> Dict[str, Any]:
    connection = open_database(
        config,
        output_reserve_bytes=track_path.stat().st_size,
        workload="bucket",
    )
    try:
        select_sql = _interval_select_sql(
            config,
            track_path,
            candidates_path,
            context_path,
            calls_path,
        )
        connection.execute(f"CREATE TEMP TABLE interval_results AS {select_sql}")
        counts: Dict[str, int] = {}
        for year in sorted(config.input.year_directories):
            output = temporary_root / f"year={year}" / "part.parquet"
            output.parent.mkdir(parents=True, exist_ok=True)
            counts[str(year)] = int(
                connection.execute(
                    parquet_copy_sql(
                        f"""
                        SELECT *
                        FROM interval_results
                        WHERE year = {year}
                        ORDER BY mmsi, start_time_utc, state
                        """,
                        output,
                        config.prepare.compression,
                        config.prepare.row_group_size,
                    )
                ).fetchone()[0]
            )
        return {
            "year_counts": counts,
            "interval_count": sum(counts.values()),
        }
    finally:
        connection.close()


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
    output_root = config.storage.work_root / "outputs" / "trajectory_intervals"
    temporary_parent = output_root / ".tmp"
    tasks: list[BucketTask] = []
    work: Dict[str, Dict[str, Any]] = {}
    completed_samples: list[tuple[int, float]] = []
    completed_count = 0
    for track_path in tracks:
        bucket = bucket_number(track_path)
        calls_bucket = calls_root / f"mmsi_bucket={bucket:04d}"
        candidates_path = calls_bucket / "candidates.parquet"
        context_path = calls_bucket / "port_context.parquet"
        calls_path = calls_bucket / "port_calls.parquet"
        dependencies = [
            track_path,
            candidates_path,
            context_path,
            calls_path,
            groups_path,
        ]
        if not all(path.exists() for path in dependencies):
            raise RuntimeError(f"Port-call outputs are incomplete for bucket {bucket:04d}")
        source_signature = signature(
            (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
            for path in dependencies
        )
        outputs = [
            output_root / f"year={year}" / f"mmsi_bucket={bucket:04d}.parquet"
            for year in sorted(config.input.year_directories)
        ]
        key = f"bucket:{bucket:04d}"
        if not force and item_is_complete(
            manifest, key, stage_hash, source_signature, outputs
        ):
            completed_count += 1
            item = manifest["items"][key]
            completed_samples.append(
                (
                    int(item.get("source_bytes", total_file_size(dependencies))),
                    float(item.get("elapsed_seconds", 0)),
                )
            )
            continue
        temporary_root = temporary_parent / f"mmsi_bucket={bucket:04d}"
        remove_path(temporary_root, config.storage.work_root)
        temporary_root.mkdir(parents=True, exist_ok=True)
        source_bytes = total_file_size(dependencies)
        work[key] = {
            "source_signature": source_signature,
            "source_bytes": source_bytes,
            "outputs": outputs,
            "temporary_root": temporary_root,
        }
        tasks.append(
            BucketTask(
                key=key,
                label=f"{bucket:04d}",
                source_bytes=source_bytes,
                estimated_output_bytes=track_path.stat().st_size,
                call=IsolatedCall(
                    key=key,
                    target=_write_interval_bucket_worker,
                    args=(
                        config,
                        track_path,
                        candidates_path,
                        context_path,
                        calls_path,
                        temporary_root,
                    ),
                ),
            )
        )

    def complete(execution: BucketExecution) -> str:
        item = work[execution.task.key]
        temporary_root = item["temporary_root"]
        for output in item["outputs"]:
            year = output.parent.name
            replace_file(temporary_root / year / "part.parquet", output)
        output_bytes = sum(output.stat().st_size for output in item["outputs"])
        remove_path(temporary_root, config.storage.work_root)
        free_after = free_space_bytes(config.storage.work_root)
        elapsed = execution.result.elapsed_seconds
        source_bytes = item["source_bytes"]
        io_rate = (source_bytes + output_bytes) / 1024**2 / elapsed
        manifest["items"][execution.task.key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": item["source_signature"],
            "outputs": [str(path.resolve()) for path in item["outputs"]],
            "interval_count": execution.result.value["interval_count"],
            "year_counts": execution.result.value["year_counts"],
            "source_bytes": source_bytes,
            "output_bytes": output_bytes,
            "io_mib_per_second": round(io_rate, 2),
            "free_space_bytes_before": execution.free_space_bytes_before,
            "free_space_bytes_after": free_after,
            "worker_process_id": execution.result.process_id,
            "elapsed_seconds": round(elapsed, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
        return f"{io_rate:.1f}MiB/s"

    run_bucket_stage(
        config,
        "build state intervals",
        tasks,
        total_count=len(tracks),
        completed_count=completed_count,
        completed_samples=completed_samples,
        on_complete=complete,
    )
    return manifest
