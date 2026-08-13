from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from extractais.calls import group_candidate_path
from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.sql import parquet_sources
from extractais.storage import (
    ensure_space,
    evidence_root_for_partition,
    lane_for_partition,
    port_call_path,
    port_context_path,
    shared_output_requirement,
    shared_temp_requirement,
    track_path,
)


INTERVAL_CONTRACT_VERSION = 2


def interval_path(config: AppConfig, year: int, partition: int) -> Path:
    return (
        config.storage.products_root
        / "trajectory_intervals"
        / f"year={year}"
        / f"partition={partition:04d}.parquet"
    )


def _year_windows(config: AppConfig) -> str:
    return ", ".join(
        f"({year}, timestamp '{year}-01-01', timestamp '{year + 1}-01-01')"
        for year in sorted(config.input.year_directories)
    )


def _interval_sql(config: AppConfig, partition: int) -> str:
    tracks = parquet_sources([track_path(config, partition)])
    candidates = parquet_sources([group_candidate_path(config, partition)])
    context = parquet_sources([port_context_path(config, partition)])
    calls = parquet_sources([port_call_path(config, partition)])
    gap_threshold = int(config.intervals.unknown_gap_hours * 3600)
    configured_years = ", ".join(
        str(year) for year in sorted(config.input.year_directories)
    )
    return f"""
        WITH valid_tracks AS (
            SELECT
                *,
                lag(timestamp_utc) OVER (
                    PARTITION BY mmsi ORDER BY point_seq
                ) AS observed_previous_time_utc
            FROM {tracks}
            WHERE NOT is_kinematic_outlier
              AND year(timestamp_utc)::INTEGER IN ({configured_years})
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
                p.port_call_id AS from_port_call_id,
                p.port_group_id AS from_port_group_id,
                p.port_group_name AS from_port_group_name,
                p.port_country_or_area AS from_port_country_or_area,
                p.entry_time_utc AS previous_entry_time_utc,
                p.exit_time_utc AS previous_exit_time_utc
            FROM track_gaps t
            ASOF LEFT JOIN calls_ordered p
              ON t.mmsi = p.mmsi AND t.timestamp_utc >= p.entry_time_utc
        ),
        with_calls AS (
            SELECT
                t.*,
                n.port_call_id AS to_port_call_id,
                n.port_group_id AS to_port_group_id,
                n.port_group_name AS to_port_group_name,
                n.port_country_or_area AS to_port_country_or_area,
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
                    WHEN from_port_call_id IS NOT NULL
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
                coalesce(candidate_margin_km < {config.ports.ambiguity_margin_km}, false)
                    AS is_port_ambiguous
            FROM candidate_context
        ),
        marked AS (
            SELECT
                *,
                CASE
                    WHEN lag(state) OVER vessel_order IS NULL
                      OR state IS DISTINCT FROM lag(state) OVER vessel_order
                      OR from_port_call_id IS DISTINCT FROM lag(from_port_call_id) OVER vessel_order
                      OR to_port_call_id IS DISTINCT FROM lag(to_port_call_id) OVER vessel_order
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
        year_windows(year, year_start, year_end) AS (
            VALUES {_year_windows(config)}
        ),
        observed_segment_bounds AS (
            SELECT
                mmsi,
                segment_number,
                min(timestamp_utc) AS start_time_utc,
                max(timestamp_utc) AS end_time_utc,
                state,
                from_port_call_id,
                from_port_group_id,
                from_port_group_name,
                from_port_country_or_area,
                to_port_call_id,
                to_port_group_id,
                to_port_group_name,
                to_port_country_or_area
            FROM grouped
            GROUP BY
                mmsi, segment_number, state,
                from_port_call_id, from_port_group_id,
                from_port_group_name, from_port_country_or_area,
                to_port_call_id, to_port_group_id,
                to_port_group_name, to_port_country_or_area
        ),
        observed_segment_year_stats AS (
            SELECT
                y.year,
                g.mmsi,
                g.segment_number,
                count(*) AS ais_point_count,
                count(speed) AS valid_speed_point_count,
                min(speed) AS min_speed_knots,
                avg(speed) AS mean_speed_knots,
                max(speed) AS max_speed_knots,
                max(observed_gap_seconds) FILTER (
                    WHERE observed_gap_seconds <= {gap_threshold}
                ) AS max_observation_gap_seconds,
                bool_or(is_time_conflict) AS has_time_conflict,
                bool_or(is_port_ambiguous) AS has_port_ambiguity,
                false AS is_unknown_gap
            FROM grouped g
            JOIN year_windows y
              ON g.timestamp_utc >= y.year_start
             AND g.timestamp_utc < y.year_end
            GROUP BY y.year, g.mmsi, g.segment_number
        ),
        observed_segments AS (
            SELECT
                stats.year,
                bounds.* EXCLUDE (
                    segment_number, start_time_utc, end_time_utc
                ),
                greatest(bounds.start_time_utc, y.year_start)
                    AS start_time_utc,
                least(bounds.end_time_utc, y.year_end)
                    AS end_time_utc,
                stats.ais_point_count,
                stats.valid_speed_point_count,
                stats.min_speed_knots,
                stats.mean_speed_knots,
                stats.max_speed_knots,
                stats.max_observation_gap_seconds,
                stats.has_time_conflict,
                stats.has_port_ambiguity,
                stats.is_unknown_gap
            FROM observed_segment_year_stats stats
            JOIN observed_segment_bounds bounds
              USING (mmsi, segment_number)
            JOIN year_windows y USING (year)
        ),
        gap_context AS (
            SELECT
                *,
                lag(from_port_call_id) OVER vessel_order AS before_gap_call_id,
                lag(from_port_group_id) OVER vessel_order AS before_gap_group_id,
                lag(from_port_group_name) OVER vessel_order AS before_gap_group_name,
                lag(from_port_country_or_area) OVER vessel_order
                    AS before_gap_country_or_area
            FROM classified
            WINDOW vessel_order AS (PARTITION BY mmsi ORDER BY point_seq)
        ),
        unknown_gaps_raw AS (
            SELECT
                mmsi,
                observed_previous_time_utc AS start_time_utc,
                timestamp_utc AS end_time_utc,
                'UNKNOWN_GAP' AS state,
                before_gap_call_id AS from_port_call_id,
                before_gap_group_id AS from_port_group_id,
                before_gap_group_name AS from_port_group_name,
                before_gap_country_or_area AS from_port_country_or_area,
                CASE WHEN state = 'IN_PORT' THEN from_port_call_id ELSE to_port_call_id END
                    AS to_port_call_id,
                CASE WHEN state = 'IN_PORT' THEN from_port_group_id ELSE to_port_group_id END
                    AS to_port_group_id,
                CASE WHEN state = 'IN_PORT' THEN from_port_group_name ELSE to_port_group_name END
                    AS to_port_group_name,
                CASE WHEN state = 'IN_PORT' THEN from_port_country_or_area ELSE to_port_country_or_area END
                    AS to_port_country_or_area,
                0::BIGINT AS ais_point_count,
                0::BIGINT AS valid_speed_point_count,
                NULL::REAL AS min_speed_knots,
                NULL::DOUBLE AS mean_speed_knots,
                NULL::REAL AS max_speed_knots,
                observed_gap_seconds AS max_observation_gap_seconds,
                false AS has_time_conflict,
                false AS has_port_ambiguity,
                true AS is_unknown_gap
            FROM gap_context
            WHERE observed_gap_seconds > {gap_threshold}
        ),
        unknown_gaps AS (
            SELECT
                y.year,
                s.* EXCLUDE (start_time_utc, end_time_utc),
                greatest(s.start_time_utc, y.year_start) AS start_time_utc,
                least(s.end_time_utc, y.year_end) AS end_time_utc
            FROM unknown_gaps_raw s
            JOIN year_windows y
              ON s.end_time_utc > y.year_start
             AND s.start_time_utc < y.year_end
            WHERE least(s.end_time_utc, y.year_end)
                > greatest(s.start_time_utc, y.year_start)
        ),
        all_segments AS (
            SELECT * FROM observed_segments
            UNION ALL BY NAME
            SELECT * FROM unknown_gaps
        ),
        quality AS (
            SELECT
                *,
                CASE
                    WHEN is_unknown_gap THEN 'NO_AIS'
                    WHEN has_time_conflict AND has_port_ambiguity THEN 'MULTIPLE_ISSUES'
                    WHEN has_port_ambiguity THEN 'PORT_AMBIGUOUS'
                    WHEN has_time_conflict THEN 'TIME_CONFLICT'
                    ELSE 'OBSERVED'
                END AS quality_flag
            FROM all_segments
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
            from_port_call_id,
            from_port_group_id,
            from_port_group_name,
            from_port_country_or_area,
            to_port_call_id,
            to_port_group_id,
            to_port_group_name,
            to_port_country_or_area,
            ais_point_count,
            valid_speed_point_count,
            min_speed_knots,
            mean_speed_knots,
            max_speed_knots,
            max_observation_gap_seconds,
            has_time_conflict,
            has_port_ambiguity,
            quality_flag,
            {partition}::INTEGER AS track_partition_id
        FROM quality
        WHERE end_time_utc >= start_time_utc
    """


def _interval_worker(
    config: AppConfig,
    partition: int,
    outputs: tuple[Path, ...],
    heartbeat_path: Path,
) -> dict[str, int]:
    temporary_outputs = tuple(path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs)
    worker_temp = config.storage.temp_root / f"intervals-{partition:04d}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    track = track_path(config, partition)
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        shared_output_requirement(config, track.stat().st_size),
        f"trajectory intervals {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"interval temporary {partition:04d}",
    )

    connection = open_database(config, worker_temp, worker=True)
    try:
        heartbeat(heartbeat_path, "classifying and compressing states")
        connection.execute(
            f"CREATE TEMP TABLE interval_results AS {_interval_sql(config, partition)}"
        )
        counts: list[int] = []
        for index, (year, output) in enumerate(
            zip(sorted(config.input.year_directories), temporary_outputs), start=1
        ):
            heartbeat(
                heartbeat_path,
                f"writing year {year} ({index}/{len(temporary_outputs)})",
            )
            counts.append(
                int(
                    connection.execute(
                        parquet_copy_sql(
                            f"SELECT * FROM interval_results WHERE year={year}",
                            output,
                            config,
                            order_by="mmsi, start_time_utc, segment_id",
                        )
                    ).fetchone()[0]
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
        "row_count": sum(counts),
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def build_intervals(config: AppConfig, store: CheckpointStore) -> None:
    interval_hash = signature(
        [
            INTERVAL_CONTRACT_VERSION,
            config.raw["ports"],
            config.raw["intervals"],
        ]
    )
    tasks: list[StageTask] = []
    for partition in range(config.layout.track_partitions):
        dependencies = [
            track_path(config, partition),
            group_candidate_path(config, partition),
            port_context_path(config, partition),
            port_call_path(config, partition),
        ]
        outputs = tuple(
            interval_path(config, year, partition)
            for year in sorted(config.input.year_directories)
        )
        task_signature = signature(
            [
                interval_hash,
                [(path.stat().st_size, path.stat().st_mtime_ns) for path in dependencies],
            ]
        )
        beat = config.storage.temp_root / "heartbeats" / f"intervals-{partition:04d}.json"
        tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=task_signature,
                source_bytes=max(1, sum(path.stat().st_size for path in dependencies)),
                outputs=outputs,
                heartbeat_path=beat,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_interval_worker,
                    args=(config, partition, outputs, beat),
                    resource=(
                        f"{lane_for_partition(config, partition)}|"
                        f"{evidence_root_for_partition(config, partition)}"
                    ),
                ),
            )
        )

    run_stage_tasks(
        config,
        store,
        "intervals",
        tasks,
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )
