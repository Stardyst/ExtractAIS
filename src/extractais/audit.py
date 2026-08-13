from __future__ import annotations

import shutil
from pathlib import Path
from typing import Any

from extractais import __version__
from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.intervals import interval_path
from extractais.isolated import IsolatedCall
from extractais.runtime import (
    StageTask,
    atomic_replace,
    heartbeat,
    run_stage_tasks,
    signature,
    write_json_atomic,
)
from extractais.sql import haversine_km, parquet_sources
from extractais.storage import (
    ensure_space,
    lane_for_partition,
    port_call_path,
    shared_temp_requirement,
    total_file_size,
    track_path,
)
from extractais.validate import validation_partial_paths


AUDIT_CONTRACT_VERSION = 1


def _temporary(path: Path) -> Path:
    return path.with_name(path.stem + ".tmp" + path.suffix)


def _track_partial_paths(config: AppConfig, partition: int) -> tuple[Path, ...]:
    root = config.storage.products_root / "quality_audit" / "partials" / "tracks"
    name = f"partition={partition:04d}.parquet"
    return (
        root / "summary" / name,
        root / "extent_bins" / name,
        root / "examples" / name,
    )


def _behavior_partial_paths(config: AppConfig, partition: int) -> tuple[Path, ...]:
    root = config.storage.products_root / "quality_audit" / "partials" / "behavior"
    name = f"partition={partition:04d}.parquet"
    return (
        root / "gap_duration" / name,
        root / "gap_coverage" / name,
        root / "state_transitions" / name,
        root / "call_summary" / name,
        root / "call_review" / name,
        root / "call_review_points" / name,
        root / "competing_pairs" / name,
        root / "interval_flags" / name,
        root / "ambiguity_assignment" / name,
    )


def _audit_outputs(config: AppConfig) -> dict[str, Path]:
    root = config.storage.products_root / "quality_audit"
    return {
        "time_conflict_summary": root / "time_conflict_summary.csv",
        "time_conflict_extent_bins": root / "time_conflict_extent_bins.csv",
        "time_conflict_examples": root / "time_conflict_examples.parquet",
        "unknown_gap_duration": root / "unknown_gap_duration.csv",
        "unknown_gap_coverage": root / "unknown_gap_coverage.csv",
        "state_transitions": root / "state_transitions.csv",
        "interval_flag_propagation": root / "interval_flag_propagation.csv",
        "ambiguity_assignment": root / "ambiguity_assignment.csv",
        "call_ambiguity_distribution": root / "call_ambiguity_distribution.csv",
        "port_call_quality": root / "port_call_quality.csv",
        "call_lifecycle": root / "call_lifecycle.csv",
        "competing_port_pairs": root / "competing_port_pairs.csv",
        "review_calls": root / "review_calls.parquet",
        "review_ambiguous_points": root / "review_ambiguous_points.parquet",
        "summary": root / "summary.json",
    }


def _copy_csv(connection, query: str, output: Path) -> None:
    connection.execute(
        f"COPY ({query}) TO {sql_literal(str(output.resolve()))} "
        "(HEADER, DELIMITER ',')"
    )


def _track_audit_worker(
    config: AppConfig,
    partition: int,
    outputs: tuple[Path, ...],
    heartbeat_path: Path,
) -> dict[str, int]:
    track = track_path(config, partition)
    temporary = tuple(_temporary(path) for path in outputs)
    worker_temp = config.storage.temp_root / f"quality-audit-track-{partition:04d}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        1024**3,
        f"track quality audit {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"track quality audit temporary {partition:04d}",
    )

    source = parquet_sources([track])
    extent = haversine_km(
        "minimum_latitude",
        "extent_minimum_longitude",
        "maximum_latitude",
        "extent_maximum_longitude",
    )
    connection = open_database(config, worker_temp, worker=True)
    try:
        heartbeat(
            heartbeat_path,
            "1/3 grouping same-second positions",
            space_path=str(config.storage.temp_root.resolve()),
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE conflict_groups AS
            WITH grouped AS (
                SELECT
                    year(timestamp_utc)::INTEGER AS year,
                    mmsi,
                    timestamp_utc,
                    count(*) AS point_count,
                    count(DISTINCT struct_pack(
                        latitude := latitude, longitude := longitude
                    )) AS coordinate_count,
                    min(latitude) AS minimum_latitude,
                    max(latitude) AS maximum_latitude,
                    min(longitude) AS minimum_longitude,
                    max(longitude) AS maximum_longitude,
                    min(CASE WHEN longitude < 0 THEN longitude + 360
                             ELSE longitude END) AS wrapped_minimum_longitude,
                    max(CASE WHEN longitude < 0 THEN longitude + 360
                             ELSE longitude END) AS wrapped_maximum_longitude
                FROM {source}
                WHERE is_time_conflict
                GROUP BY year, mmsi, timestamp_utc
            ),
            normalized AS (
                SELECT
                    *,
                    CASE
                        WHEN maximum_longitude - minimum_longitude
                           <= wrapped_maximum_longitude
                            - wrapped_minimum_longitude
                        THEN minimum_longitude
                        ELSE wrapped_minimum_longitude
                    END AS extent_minimum_longitude,
                    CASE
                        WHEN maximum_longitude - minimum_longitude
                           <= wrapped_maximum_longitude
                            - wrapped_minimum_longitude
                        THEN maximum_longitude
                        ELSE wrapped_maximum_longitude
                    END AS extent_maximum_longitude
                FROM grouped
            ),
            measured AS (
                SELECT *, {extent} AS spatial_extent_proxy_km
                FROM normalized
            )
            SELECT
                *,
                CASE
                    WHEN spatial_extent_proxy_km <= 0.1 THEN 'LE_100_M'
                    WHEN spatial_extent_proxy_km <= 1.0 THEN '100_M_TO_1_KM'
                    WHEN spatial_extent_proxy_km <= 10.0 THEN '01_TO_10_KM'
                    ELSE 'GT_10_KM'
                END AS spatial_extent_bin
            FROM measured
            """
        )

        heartbeat(
            heartbeat_path,
            "2/3 writing point-level summary",
            progress_path=str(temporary[0].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        summary_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    WITH point_summary AS (
                        SELECT
                            year(timestamp_utc)::INTEGER AS year,
                            count(*) AS point_count,
                            count(DISTINCT mmsi) AS vessel_count,
                            count(*) FILTER (WHERE is_time_conflict)
                                AS flagged_time_conflict_point_count
                        FROM {source}
                        GROUP BY year
                    ),
                    conflict_summary AS (
                        SELECT
                            year,
                            count(*) AS time_conflict_timestamp_count,
                            sum(point_count) AS conflict_group_point_count
                        FROM conflict_groups
                        GROUP BY year
                    )
                    SELECT
                        p.year,
                        {partition}::INTEGER AS track_partition_id,
                        p.point_count,
                        p.vessel_count,
                        p.flagged_time_conflict_point_count,
                        coalesce(c.time_conflict_timestamp_count, 0)
                            AS time_conflict_timestamp_count,
                        coalesce(c.conflict_group_point_count, 0)
                            AS conflict_group_point_count
                    FROM point_summary p
                    LEFT JOIN conflict_summary c USING (year)
                    """,
                    temporary[0],
                    config,
                    order_by="year",
                )
            ).fetchone()[0]
        )

        heartbeat(
            heartbeat_path,
            "3/3 writing conflict extent evidence",
            progress_path=str(temporary[1].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        bin_count = int(
            connection.execute(
                parquet_copy_sql(
                    """
                    SELECT
                        spatial_extent_bin,
                        count(*) AS time_conflict_timestamp_count,
                        sum(point_count) AS flagged_point_count,
                        max(spatial_extent_proxy_km) AS maximum_extent_proxy_km,
                        year
                    FROM conflict_groups
                    GROUP BY spatial_extent_bin, year
                    """,
                    temporary[1],
                    config,
                    order_by="year, spatial_extent_bin",
                )
            ).fetchone()[0]
        )
        example_count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT * EXCLUDE (example_rank)
                    FROM (
                        SELECT
                            {partition}::INTEGER AS track_partition_id,
                            *,
                            row_number() OVER (
                                PARTITION BY year, spatial_extent_bin
                                ORDER BY spatial_extent_proxy_km DESC,
                                         mmsi, timestamp_utc
                            ) AS example_rank
                        FROM conflict_groups
                    )
                    WHERE example_rank <= 20
                    """,
                    temporary[2],
                    config,
                    order_by=(
                        "year, spatial_extent_bin, "
                        "spatial_extent_proxy_km DESC, mmsi, timestamp_utc"
                    ),
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)

    for source_path, output in zip(temporary, outputs):
        atomic_replace(source_path, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": summary_count + bin_count + example_count,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def _behavior_audit_worker(
    config: AppConfig,
    partition: int,
    outputs: tuple[Path, ...],
    heartbeat_path: Path,
) -> dict[str, int]:
    intervals = [
        interval_path(config, year, partition)
        for year in sorted(config.input.year_directories)
    ]
    calls = port_call_path(config, partition)
    ambiguous = validation_partial_paths(config, partition)[2]
    temporary = tuple(_temporary(path) for path in outputs)
    worker_temp = config.storage.temp_root / f"quality-audit-behavior-{partition:04d}"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        max(1024**3, ambiguous.stat().st_size),
        f"behavior quality audit {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"behavior quality audit temporary {partition:04d}",
    )

    interval_source = parquet_sources(intervals)
    call_source = parquet_sources([calls])
    ambiguous_source = parquet_sources([ambiguous])
    current_margin = config.ports.ambiguity_margin_km
    connection = open_database(config, worker_temp, worker=True)
    try:
        heartbeat(
            heartbeat_path,
            "1/8 loading interval and call evidence",
            space_path=str(config.storage.temp_root.resolve()),
        )
        connection.execute(
            f"CREATE TEMP VIEW audit_intervals AS SELECT * FROM {interval_source}"
        )
        connection.execute(
            f"CREATE TEMP VIEW audit_calls AS SELECT * FROM {call_source}"
        )
        connection.execute(
            f"CREATE TEMP VIEW ambiguous_points AS SELECT * FROM {ambiguous_source}"
        )

        heartbeat(
            heartbeat_path,
            "2/8 assigning ambiguous points to confirmed calls",
            space_path=str(config.storage.temp_root.resolve()),
        )
        connection.execute(
            """
            CREATE TEMP TABLE assigned_ambiguous AS
            SELECT
                a.*,
                c.port_call_id,
                year(c.entry_time_utc)::INTEGER AS call_year
            FROM ambiguous_points a
            ASOF JOIN audit_calls c
              ON a.mmsi = c.mmsi
             AND a.port_group_id = c.port_group_id
             AND a.timestamp_utc >= c.approach_start_time_utc
            WHERE a.timestamp_utc <= c.approach_end_time_utc
            """
        )
        connection.execute(
            f"""
            CREATE TEMP TABLE ambiguous_by_call AS
            SELECT
                port_call_id,
                count(*) AS ambiguous_point_count,
                count(*) FILTER (WHERE ambiguity_margin_km <= 0.10)
                    AS ambiguous_points_le_0_10_km,
                count(*) FILTER (WHERE ambiguity_margin_km <= 0.25)
                    AS ambiguous_points_le_0_25_km,
                count(*) FILTER (WHERE ambiguity_margin_km <= 0.50)
                    AS ambiguous_points_le_0_50_km,
                min(ambiguity_margin_km) AS minimum_recomputed_margin_km,
                quantile_cont(ambiguity_margin_km, 0.5)
                    AS median_ambiguous_margin_km,
                arg_min(second_port_group_id, struct_pack(
                    margin := ambiguity_margin_km,
                    observed_at := timestamp_utc
                )) AS closest_competing_port_group_id,
                arg_min(second_port_group_name, struct_pack(
                    margin := ambiguity_margin_km,
                    observed_at := timestamp_utc
                )) AS closest_competing_port_group_name
            FROM assigned_ambiguous
            WHERE ambiguity_margin_km < {current_margin}
            GROUP BY port_call_id
            """
        )

        heartbeat(
            heartbeat_path,
            "3/8 evaluating call lifecycle coverage",
            space_path=str(config.storage.temp_root.resolve()),
        )
        connection.execute(
            """
            CREATE TEMP TABLE call_lifecycle AS
            WITH call_refs AS (
                SELECT
                    to_port_call_id AS port_call_id,
                    state = 'ARRIVING' AS is_arriving,
                    state = 'IN_PORT' AS is_in_port,
                    false AS is_departing
                FROM audit_intervals
                WHERE to_port_call_id IS NOT NULL
                UNION ALL
                SELECT
                    from_port_call_id AS port_call_id,
                    false AS is_arriving,
                    state = 'IN_PORT' AS is_in_port,
                    state = 'DEPARTING' AS is_departing
                FROM audit_intervals
                WHERE from_port_call_id IS NOT NULL
            )
            SELECT
                port_call_id,
                bool_or(is_arriving) AS has_arriving_interval,
                bool_or(is_in_port) AS has_in_port_interval,
                bool_or(is_departing) AS has_departing_interval
            FROM call_refs
            GROUP BY port_call_id
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE call_quality AS
            SELECT
                year(c.entry_time_utc)::INTEGER AS year,
                c.port_call_id,
                c.mmsi,
                c.track_partition_id,
                c.port_group_id,
                c.port_group_name,
                c.port_country_or_area,
                c.approach_start_time_utc,
                c.approach_end_time_utc,
                c.entry_time_utc,
                c.exit_time_utc,
                c.point_count,
                c.minimum_port_distance_km,
                c.minimum_ambiguity_margin_km,
                c.has_port_ambiguity,
                coalesce(a.ambiguous_point_count, 0) AS ambiguous_point_count,
                coalesce(a.ambiguous_point_count, 0)::DOUBLE
                    / nullif(c.point_count, 0) AS ambiguous_point_fraction,
                CASE
                    WHEN coalesce(a.ambiguous_point_count, 0) = 0 THEN 'ZERO'
                    WHEN a.ambiguous_point_count::DOUBLE / c.point_count <= 0.01
                        THEN 'GT_00_TO_01_PERCENT'
                    WHEN a.ambiguous_point_count::DOUBLE / c.point_count <= 0.10
                        THEN 'GT_01_TO_10_PERCENT'
                    WHEN a.ambiguous_point_count::DOUBLE / c.point_count <= 0.50
                        THEN 'GT_10_TO_50_PERCENT'
                    ELSE 'GT_50_PERCENT'
                END AS ambiguity_fraction_bin,
                coalesce(a.ambiguous_points_le_0_10_km, 0)
                    AS ambiguous_points_le_0_10_km,
                coalesce(a.ambiguous_points_le_0_25_km, 0)
                    AS ambiguous_points_le_0_25_km,
                coalesce(a.ambiguous_points_le_0_50_km, 0)
                    AS ambiguous_points_le_0_50_km,
                a.minimum_recomputed_margin_km,
                a.median_ambiguous_margin_km,
                a.closest_competing_port_group_id,
                a.closest_competing_port_group_name,
                coalesce(l.has_arriving_interval, false) AS has_arriving_interval,
                coalesce(l.has_in_port_interval, false) AS has_in_port_interval,
                coalesce(l.has_departing_interval, false) AS has_departing_interval,
                c.has_port_ambiguity IS DISTINCT FROM
                    (coalesce(a.ambiguous_point_count, 0) > 0)
                    AS ambiguity_flag_mismatch
            FROM audit_calls c
            LEFT JOIN ambiguous_by_call a USING (port_call_id)
            LEFT JOIN call_lifecycle l USING (port_call_id)
            """
        )

        heartbeat(
            heartbeat_path,
            "4/8 summarizing unknown-gap duration",
            progress_path=str(temporary[0].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        counts: list[int] = []
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        WITH gaps AS (
                            SELECT
                                year,
                                mmsi,
                                date_diff('second', start_time_utc, end_time_utc)
                                    AS gap_seconds
                            FROM audit_intervals
                            WHERE state = 'UNKNOWN_GAP'
                        )
                        SELECT
                            year,
                            CASE
                                WHEN gap_seconds <= 12 * 3600 THEN '06_TO_12_HOURS'
                                WHEN gap_seconds <= 24 * 3600 THEN '12_TO_24_HOURS'
                                WHEN gap_seconds <= 3 * 86400 THEN '01_TO_03_DAYS'
                                WHEN gap_seconds <= 7 * 86400 THEN '03_TO_07_DAYS'
                                ELSE 'GT_07_DAYS'
                            END AS duration_bin,
                            count(*) AS gap_count,
                            sum(gap_seconds) AS total_gap_seconds,
                            count(DISTINCT mmsi) AS vessel_count,
                            min(gap_seconds) AS minimum_gap_seconds,
                            max(gap_seconds) AS maximum_gap_seconds
                        FROM gaps
                        GROUP BY year, duration_bin
                        """,
                        temporary[0],
                        config,
                        order_by="year, duration_bin",
                    )
                ).fetchone()[0]
            )
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        WITH by_vessel AS (
                            SELECT
                                year,
                                mmsi,
                                date_diff(
                                    'second', min(start_time_utc), max(end_time_utc)
                                ) AS active_span_seconds,
                                sum(
                                    CASE WHEN state = 'UNKNOWN_GAP'
                                         THEN date_diff(
                                             'second', start_time_utc, end_time_utc
                                         ) ELSE 0 END
                                ) AS gap_seconds
                            FROM audit_intervals
                            GROUP BY year, mmsi
                        ),
                        measured AS (
                            SELECT
                                *,
                                gap_seconds::DOUBLE / nullif(active_span_seconds, 0)
                                    AS gap_fraction
                            FROM by_vessel
                        )
                        SELECT
                            year,
                            CASE
                                WHEN active_span_seconds = 0 THEN 'NO_SPAN'
                                WHEN gap_fraction <= 0.10 THEN 'LE_10_PERCENT'
                                WHEN gap_fraction <= 0.25 THEN '10_TO_25_PERCENT'
                                WHEN gap_fraction <= 0.50 THEN '25_TO_50_PERCENT'
                                ELSE 'GT_50_PERCENT'
                            END AS coverage_bin,
                            count(*) AS vessel_count,
                            sum(active_span_seconds) AS total_active_span_seconds,
                            sum(gap_seconds) AS total_gap_seconds
                        FROM measured
                        GROUP BY year, coverage_bin
                        """,
                        temporary[1],
                        config,
                        order_by="year, coverage_bin",
                    )
                ).fetchone()[0]
            )
        )

        heartbeat(
            heartbeat_path,
            "5/8 collapsing and counting state transitions",
            progress_path=str(temporary[2].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        WITH ordered AS (
                            SELECT
                                *,
                                CASE WHEN state IS DISTINCT FROM lag(state) OVER (
                                    PARTITION BY year, mmsi
                                    ORDER BY start_time_utc, segment_id
                                ) THEN 1 ELSE 0 END AS new_state_run
                            FROM audit_intervals
                        ),
                        numbered AS (
                            SELECT
                                *,
                                sum(new_state_run) OVER (
                                    PARTITION BY year, mmsi
                                    ORDER BY start_time_utc, segment_id
                                    ROWS UNBOUNDED PRECEDING
                                ) AS state_run
                            FROM ordered
                        ),
                        collapsed AS (
                            SELECT
                                year, mmsi, state_run,
                                arg_min(state, start_time_utc) AS state,
                                min(start_time_utc) AS run_start_time_utc
                            FROM numbered
                            GROUP BY year, mmsi, state_run
                        ),
                        transitions AS (
                            SELECT
                                year,
                                mmsi,
                                state AS from_state,
                                lead(state) OVER (
                                    PARTITION BY year, mmsi
                                    ORDER BY run_start_time_utc, state_run
                                ) AS to_state
                            FROM collapsed
                        )
                        SELECT
                            year, from_state, to_state,
                            count(*) AS transition_count,
                            count(DISTINCT mmsi) AS vessel_count
                        FROM transitions
                        WHERE to_state IS NOT NULL
                        GROUP BY year, from_state, to_state
                        """,
                        temporary[2],
                        config,
                        order_by="year, from_state, to_state",
                    )
                ).fetchone()[0]
            )
        )

        heartbeat(
            heartbeat_path,
            "6/8 writing call-level diagnostics",
            progress_path=str(temporary[3].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        SELECT
                            year,
                            port_group_id,
                            arg_min(port_group_name, entry_time_utc)
                                AS port_group_name,
                            arg_min(port_country_or_area, entry_time_utc)
                                AS port_country_or_area,
                            ambiguity_fraction_bin,
                            has_port_ambiguity,
                            has_arriving_interval,
                            has_in_port_interval,
                            has_departing_interval,
                            count(*) AS call_count,
                            sum(point_count) AS call_point_count,
                            sum(ambiguous_point_count) AS ambiguous_point_count,
                            count(*) FILTER (WHERE ambiguous_point_count > 0)
                                AS recomputed_ambiguous_call_count,
                            sum(ambiguous_points_le_0_10_km)
                                AS ambiguous_points_le_0_10_km,
                            sum(ambiguous_points_le_0_25_km)
                                AS ambiguous_points_le_0_25_km,
                            sum(ambiguous_points_le_0_50_km)
                                AS ambiguous_points_le_0_50_km,
                            count(*) FILTER (WHERE ambiguity_flag_mismatch)
                                AS ambiguity_flag_mismatch_count
                        FROM call_quality
                        GROUP BY
                            year, port_group_id, ambiguity_fraction_bin,
                            has_port_ambiguity, has_arriving_interval,
                            has_in_port_interval, has_departing_interval
                        """,
                        temporary[3],
                        config,
                        order_by="year, port_group_id, ambiguity_fraction_bin",
                    )
                ).fetchone()[0]
            )
        )
        review_sql = """
            WITH candidates AS (
                SELECT 'HIGH_FRACTION' AS review_stratum, *
                FROM call_quality
                WHERE ambiguous_point_count > 0
                QUALIFY row_number() OVER (
                    ORDER BY ambiguous_point_fraction DESC,
                             ambiguous_point_count DESC, port_call_id
                ) <= 200
                UNION ALL
                SELECT 'HIGH_COUNT' AS review_stratum, *
                FROM call_quality
                WHERE ambiguous_point_count > 0
                QUALIFY row_number() OVER (
                    ORDER BY ambiguous_point_count DESC,
                             ambiguous_point_fraction DESC, port_call_id
                ) <= 200
                UNION ALL
                SELECT 'CONTROL' AS review_stratum, *
                FROM call_quality
                WHERE ambiguous_point_count = 0
                QUALIFY row_number() OVER (
                    ORDER BY hash(port_call_id)
                ) <= 200
            ),
            deduplicated AS (
                SELECT *
                FROM candidates
                QUALIFY row_number() OVER (
                    PARTITION BY port_call_id
                    ORDER BY CASE review_stratum
                        WHEN 'HIGH_FRACTION' THEN 1
                        WHEN 'HIGH_COUNT' THEN 2 ELSE 3 END
                ) = 1
            )
            SELECT * FROM deduplicated
        """
        connection.execute(f"CREATE TEMP TABLE review_calls AS {review_sql}")
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        "SELECT * FROM review_calls",
                        temporary[4],
                        config,
                        order_by="review_stratum, year, port_call_id",
                    )
                ).fetchone()[0]
            )
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        SELECT * EXCLUDE (
                            closest_rank, first_rank, last_rank
                        )
                        FROM (
                        SELECT
                            r.review_stratum,
                            a.port_call_id,
                            a.mmsi,
                            a.point_seq,
                            a.timestamp_utc,
                            a.latitude,
                            a.longitude,
                            a.port_group_id,
                            a.port_group_name,
                            a.second_port_group_id,
                            a.second_port_group_name,
                            a.port_distance_km,
                            a.second_port_distance_km,
                            a.ambiguity_margin_km,
                            a.track_partition_id,
                            row_number() OVER (
                                PARTITION BY a.port_call_id
                                ORDER BY a.ambiguity_margin_km,
                                         a.timestamp_utc, a.point_seq
                            ) AS closest_rank,
                            row_number() OVER (
                                PARTITION BY a.port_call_id
                                ORDER BY a.timestamp_utc, a.point_seq
                            ) AS first_rank,
                            row_number() OVER (
                                PARTITION BY a.port_call_id
                                ORDER BY a.timestamp_utc DESC, a.point_seq DESC
                            ) AS last_rank
                        FROM assigned_ambiguous a
                        JOIN review_calls r USING (port_call_id)
                        )
                        WHERE closest_rank <= 10
                           OR first_rank <= 5
                           OR last_rank <= 5
                        """,
                        temporary[5],
                        config,
                        order_by="review_stratum, port_call_id, point_seq",
                    )
                ).fetchone()[0]
            )
        )

        heartbeat(
            heartbeat_path,
            "7/8 aggregating competing-port pairs",
            progress_path=str(temporary[6].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        SELECT
                            call_year AS year,
                            port_group_id,
                            arg_min(port_group_name, timestamp_utc)
                                AS port_group_name,
                            second_port_group_id,
                            arg_min(second_port_group_name, timestamp_utc)
                                AS second_port_group_name,
                            count(*) AS ambiguous_point_count,
                            count(DISTINCT port_call_id) AS affected_call_count,
                            count(DISTINCT mmsi) AS affected_vessel_count,
                            min(ambiguity_margin_km) AS minimum_margin_km,
                            avg(ambiguity_margin_km) AS mean_margin_km
                        FROM assigned_ambiguous
                        WHERE second_port_group_id IS NOT NULL
                        GROUP BY
                            call_year, port_group_id, second_port_group_id
                        """,
                        temporary[6],
                        config,
                        order_by=(
                            "year, ambiguous_point_count DESC, "
                            "port_group_id, second_port_group_id"
                        ),
                    )
                ).fetchone()[0]
            )
        )

        heartbeat(
            heartbeat_path,
            "8/8 measuring flag propagation and assignment coverage",
            progress_path=str(temporary[7].resolve()),
            space_path=str(config.storage.products_root.resolve()),
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        SELECT
                            year, state, has_time_conflict, has_port_ambiguity,
                            count(*) AS interval_count,
                            sum(ais_point_count) AS ais_point_count,
                            sum(date_diff(
                                'second', start_time_utc, end_time_utc
                            )) AS interval_duration_seconds
                        FROM audit_intervals
                        GROUP BY
                            year, state, has_time_conflict, has_port_ambiguity
                        """,
                        temporary[7],
                        config,
                        order_by=(
                            "year, state, has_time_conflict, has_port_ambiguity"
                        ),
                    )
                ).fetchone()[0]
            )
        )
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        """
                        WITH all_points AS (
                            SELECT
                                year(timestamp_utc)::INTEGER AS year,
                                count(*) AS ambiguous_point_count
                            FROM ambiguous_points
                            GROUP BY year
                        ),
                        assigned AS (
                            SELECT
                                year(timestamp_utc)::INTEGER AS year,
                                count(*) AS assigned_point_count
                            FROM assigned_ambiguous
                            GROUP BY year
                        )
                        SELECT
                            coalesce(a.year, b.year) AS year,
                            coalesce(a.ambiguous_point_count, 0)
                                AS ambiguous_point_count,
                            coalesce(b.assigned_point_count, 0)
                                AS assigned_point_count,
                            coalesce(a.ambiguous_point_count, 0)
                              - coalesce(b.assigned_point_count, 0)
                                AS unassigned_point_count
                        FROM all_points a
                        FULL OUTER JOIN assigned b USING (year)
                        """,
                        temporary[8],
                        config,
                        order_by="year",
                    )
                ).fetchone()[0]
            )
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)

    for source_path, output in zip(temporary, outputs):
        atomic_replace(source_path, output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": sum(counts),
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def _merge_audit_worker(
    config: AppConfig,
    track_partials: list[tuple[Path, ...]],
    behavior_partials: list[tuple[Path, ...]],
    outputs: tuple[Path, ...],
    heartbeat_path: Path,
) -> dict[str, int]:
    current_margin = config.ports.ambiguity_margin_km
    output_names = list(_audit_outputs(config))
    final = dict(zip(output_names, outputs))
    temporary = {name: _temporary(path) for name, path in final.items()}
    worker_temp = config.storage.temp_root / "quality-audit-merge"
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary.values():
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    review_partial_bytes = total_file_size(
        [paths[5] for paths in behavior_partials]
    )
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        max(2 * 1024**3, review_partial_bytes),
        "quality audit reports",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        "quality audit merge temporary",
    )

    track_summary_paths = [paths[0] for paths in track_partials]
    track_bin_paths = [paths[1] for paths in track_partials]
    track_example_paths = [paths[2] for paths in track_partials]
    behavior_columns = list(zip(*behavior_partials))
    (
        gap_paths,
        coverage_paths,
        transition_paths,
        call_summary_paths,
        call_review_paths,
        call_review_point_paths,
        pair_paths,
        interval_flag_paths,
        assignment_paths,
    ) = (list(paths) for paths in behavior_columns)
    groups = config.storage.products_root / "ports" / "port_groups.parquet"

    connection = open_database(config, worker_temp, worker=False)
    row_count = 0
    try:
        heartbeat(heartbeat_path, "1/14 merging time-conflict summary")
        _copy_csv(
            connection,
            f"""
            SELECT
                year,
                sum(point_count) AS point_count,
                sum(vessel_count) AS vessel_count,
                sum(flagged_time_conflict_point_count)
                    AS flagged_time_conflict_point_count,
                sum(flagged_time_conflict_point_count)::DOUBLE
                    / nullif(sum(point_count), 0)
                    AS flagged_time_conflict_point_rate,
                sum(time_conflict_timestamp_count)
                    AS time_conflict_timestamp_count
            FROM {parquet_sources(track_summary_paths)}
            GROUP BY year
            ORDER BY year
            """,
            temporary["time_conflict_summary"],
        )

        heartbeat(heartbeat_path, "2/14 merging spatial conflict bins")
        _copy_csv(
            connection,
            f"""
            SELECT
                year,
                spatial_extent_bin,
                sum(time_conflict_timestamp_count)
                    AS time_conflict_timestamp_count,
                sum(flagged_point_count) AS flagged_point_count,
                max(maximum_extent_proxy_km) AS maximum_extent_proxy_km
            FROM {parquet_sources(track_bin_paths)}
            GROUP BY year, spatial_extent_bin
            ORDER BY year, spatial_extent_bin
            """,
            temporary["time_conflict_extent_bins"],
        )
        row_count += int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT * EXCLUDE (global_rank)
                    FROM (
                        SELECT
                            *,
                            row_number() OVER (
                                PARTITION BY year, spatial_extent_bin
                                ORDER BY spatial_extent_proxy_km DESC,
                                         mmsi, timestamp_utc
                            ) AS global_rank
                        FROM {parquet_sources(track_example_paths)}
                    )
                    WHERE global_rank <= 100
                    """,
                    temporary["time_conflict_examples"],
                    config,
                    order_by=(
                        "year, spatial_extent_bin, "
                        "spatial_extent_proxy_km DESC, mmsi, timestamp_utc"
                    ),
                )
            ).fetchone()[0]
        )

        heartbeat(heartbeat_path, "3/14 merging unknown-gap duration")
        _copy_csv(
            connection,
            f"""
            SELECT
                year, duration_bin,
                sum(gap_count) AS gap_count,
                sum(total_gap_seconds) AS total_gap_seconds,
                sum(vessel_count) AS vessel_count,
                min(minimum_gap_seconds) AS minimum_gap_seconds,
                max(maximum_gap_seconds) AS maximum_gap_seconds
            FROM {parquet_sources(gap_paths)}
            GROUP BY year, duration_bin
            ORDER BY year, duration_bin
            """,
            temporary["unknown_gap_duration"],
        )
        _copy_csv(
            connection,
            f"""
            SELECT
                year, coverage_bin,
                sum(vessel_count) AS vessel_count,
                sum(total_active_span_seconds) AS total_active_span_seconds,
                sum(total_gap_seconds) AS total_gap_seconds,
                sum(total_gap_seconds)::DOUBLE
                    / nullif(sum(total_active_span_seconds), 0)
                    AS weighted_gap_fraction
            FROM {parquet_sources(coverage_paths)}
            GROUP BY year, coverage_bin
            ORDER BY year, coverage_bin
            """,
            temporary["unknown_gap_coverage"],
        )

        heartbeat(heartbeat_path, "4/14 merging state transitions")
        _copy_csv(
            connection,
            f"""
            SELECT
                year, from_state, to_state,
                sum(transition_count) AS transition_count,
                sum(vessel_count) AS vessel_count
            FROM {parquet_sources(transition_paths)}
            GROUP BY year, from_state, to_state
            ORDER BY year, from_state, to_state
            """,
            temporary["state_transitions"],
        )

        heartbeat(heartbeat_path, "5/14 measuring interval flag propagation")
        _copy_csv(
            connection,
            f"""
            SELECT
                year, state, has_time_conflict, has_port_ambiguity,
                sum(interval_count) AS interval_count,
                sum(ais_point_count) AS ais_point_count,
                sum(interval_duration_seconds) AS interval_duration_seconds
            FROM {parquet_sources(interval_flag_paths)}
            GROUP BY year, state, has_time_conflict, has_port_ambiguity
            ORDER BY year, state, has_time_conflict, has_port_ambiguity
            """,
            temporary["interval_flag_propagation"],
        )

        heartbeat(heartbeat_path, "6/14 checking ambiguity assignment coverage")
        _copy_csv(
            connection,
            f"""
            SELECT
                year,
                sum(ambiguous_point_count) AS ambiguous_point_count,
                sum(assigned_point_count) AS assigned_point_count,
                sum(unassigned_point_count) AS unassigned_point_count,
                sum(assigned_point_count)::DOUBLE
                    / nullif(sum(ambiguous_point_count), 0)
                    AS assignment_rate
            FROM {parquet_sources(assignment_paths)}
            GROUP BY year
            ORDER BY year
            """,
            temporary["ambiguity_assignment"],
        )

        call_summaries = parquet_sources(call_summary_paths)
        heartbeat(heartbeat_path, "7/14 merging call ambiguity distribution")
        _copy_csv(
            connection,
            f"""
            SELECT
                year, ambiguity_fraction_bin,
                sum(call_count) AS call_count,
                sum(call_point_count) AS call_point_count,
                sum(ambiguous_point_count) AS ambiguous_point_count,
                sum(recomputed_ambiguous_call_count)
                    AS recomputed_ambiguous_call_count,
                sum(call_count) FILTER (WHERE has_port_ambiguity)
                    AS source_ambiguous_call_count,
                sum(ambiguity_flag_mismatch_count)
                    AS ambiguity_flag_mismatch_count
            FROM {call_summaries}
            GROUP BY year, ambiguity_fraction_bin
            ORDER BY year, ambiguity_fraction_bin
            """,
            temporary["call_ambiguity_distribution"],
        )

        heartbeat(heartbeat_path, "8/14 building per-port call quality")
        _copy_csv(
            connection,
            f"""
            SELECT
                year,
                port_group_id,
                arg_min(port_group_name, ambiguity_fraction_bin)
                    AS port_group_name,
                arg_min(port_country_or_area, ambiguity_fraction_bin)
                    AS port_country_or_area,
                sum(call_count) AS call_count,
                sum(recomputed_ambiguous_call_count)
                    AS recomputed_ambiguous_call_count,
                sum(recomputed_ambiguous_call_count)::DOUBLE
                    / nullif(sum(call_count), 0) AS ambiguous_call_rate,
                sum(call_point_count) AS call_point_count,
                sum(ambiguous_point_count) AS ambiguous_point_count,
                sum(ambiguous_point_count)::DOUBLE
                    / nullif(sum(call_point_count), 0) AS ambiguous_point_rate,
                sum(ambiguous_points_le_0_10_km)
                    AS ambiguous_points_le_0_10_km,
                sum(ambiguous_points_le_0_25_km)
                    AS ambiguous_points_le_0_25_km,
                sum(ambiguous_points_le_0_50_km)
                    AS ambiguous_points_le_0_50_km,
                sum(call_count) FILTER (
                    WHERE has_arriving_interval
                      AND has_in_port_interval
                      AND has_departing_interval
                ) AS complete_lifecycle_call_count,
                sum(ambiguity_flag_mismatch_count)
                    AS ambiguity_flag_mismatch_count
            FROM {call_summaries}
            GROUP BY year, port_group_id
            ORDER BY year, ambiguous_point_count DESC, port_group_id
            """,
            temporary["port_call_quality"],
        )

        heartbeat(heartbeat_path, "9/14 merging call lifecycle patterns")
        _copy_csv(
            connection,
            f"""
            SELECT
                year,
                has_arriving_interval,
                has_in_port_interval,
                has_departing_interval,
                sum(call_count) AS call_count
            FROM {call_summaries}
            GROUP BY
                year, has_arriving_interval,
                has_in_port_interval, has_departing_interval
            ORDER BY
                year, has_arriving_interval DESC,
                has_in_port_interval DESC, has_departing_interval DESC
            """,
            temporary["call_lifecycle"],
        )

        heartbeat(heartbeat_path, "10/14 ranking competing port groups")
        group_source = parquet_sources([groups])
        group_distance = haversine_km(
            "origin.latitude",
            "origin.longitude",
            "competitor.latitude",
            "competitor.longitude",
        )
        _copy_csv(
            connection,
            f"""
            WITH pairs AS (
                SELECT
                    year,
                    port_group_id,
                    arg_min(port_group_name, second_port_group_id)
                        AS port_group_name,
                    second_port_group_id,
                    arg_min(second_port_group_name, port_group_id)
                        AS second_port_group_name,
                    sum(ambiguous_point_count) AS ambiguous_point_count,
                    sum(affected_call_count) AS affected_call_count,
                    sum(affected_vessel_count) AS affected_vessel_count,
                    min(minimum_margin_km) AS minimum_margin_km,
                    sum(mean_margin_km * ambiguous_point_count)
                        / nullif(sum(ambiguous_point_count), 0) AS mean_margin_km
                FROM {parquet_sources(pair_paths)}
                GROUP BY year, port_group_id, second_port_group_id
            ),
            enriched AS (
                SELECT
                    p.*,
                    origin.country_or_area_name AS port_country_or_area,
                    origin.harbor_size AS port_harbor_size,
                    competitor.country_or_area_name
                        AS second_port_country_or_area,
                    competitor.harbor_size AS second_port_harbor_size,
                    {group_distance} AS representative_distance_km
                FROM pairs p
                LEFT JOIN {group_source} origin
                  ON p.port_group_id = origin.port_group_id
                LEFT JOIN {group_source} competitor
                  ON p.second_port_group_id = competitor.port_group_id
            )
            SELECT
                *,
                ambiguous_point_count::DOUBLE / nullif(
                    sum(ambiguous_point_count) OVER (
                        PARTITION BY year, port_group_id
                    ), 0
                ) AS origin_ambiguity_share
            FROM enriched
            ORDER BY year, ambiguous_point_count DESC,
                     port_group_id, second_port_group_id
            """,
            temporary["competing_port_pairs"],
        )

        heartbeat(heartbeat_path, "11/14 selecting deterministic review calls")
        row_count += int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    WITH candidates AS (
                        SELECT *
                        FROM {parquet_sources(call_review_paths)}
                        QUALIFY row_number() OVER (
                            PARTITION BY port_call_id
                            ORDER BY CASE review_stratum
                                WHEN 'HIGH_FRACTION' THEN 1
                                WHEN 'HIGH_COUNT' THEN 2 ELSE 3 END
                        ) = 1
                    ),
                    ranked AS (
                        SELECT
                            *,
                            row_number() OVER (
                                PARTITION BY review_stratum
                                ORDER BY
                                    CASE review_stratum
                                        WHEN 'HIGH_FRACTION'
                                            THEN ambiguous_point_fraction
                                        WHEN 'HIGH_COUNT'
                                            THEN ambiguous_point_count::DOUBLE
                                        ELSE 0.0
                                    END DESC,
                                    hash(port_call_id)
                            ) AS sample_rank
                        FROM candidates
                    )
                    SELECT * FROM ranked WHERE sample_rank <= 200
                    """,
                    temporary["review_calls"],
                    config,
                    order_by="review_stratum, sample_rank, port_call_id",
                )
            ).fetchone()[0]
        )

        heartbeat(heartbeat_path, "12/14 collecting review-point evidence")
        row_count += int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT
                        p.*,
                        selected.latitude AS selected_port_latitude,
                        selected.longitude AS selected_port_longitude,
                        selected.country_or_area_name
                            AS selected_port_country_or_area,
                        selected.harbor_size AS selected_port_harbor_size,
                        competitor.latitude AS competing_port_latitude,
                        competitor.longitude AS competing_port_longitude,
                        competitor.country_or_area_name
                            AS competing_port_country_or_area,
                        competitor.harbor_size AS competing_port_harbor_size
                    FROM {parquet_sources(call_review_point_paths)} p
                    JOIN {parquet_sources([temporary['review_calls']])} r
                      USING (port_call_id)
                    LEFT JOIN {group_source} selected
                      ON p.port_group_id = selected.port_group_id
                    LEFT JOIN {group_source} competitor
                      ON p.second_port_group_id = competitor.port_group_id
                    """,
                    temporary["review_ambiguous_points"],
                    config,
                    order_by="review_stratum, port_call_id, point_seq",
                )
            ).fetchone()[0]
        )

        heartbeat(heartbeat_path, "13/14 calculating headline metrics")
        track_metrics = connection.execute(
            f"""
            SELECT
                sum(point_count),
                sum(flagged_time_conflict_point_count),
                sum(time_conflict_timestamp_count)
            FROM {parquet_sources(track_summary_paths)}
            """
        ).fetchone()
        gap_metrics = connection.execute(
            f"""
            SELECT sum(total_active_span_seconds), sum(total_gap_seconds)
            FROM {parquet_sources(coverage_paths)}
            """
        ).fetchone()
        call_metrics = connection.execute(
            f"""
            SELECT
                sum(call_count),
                sum(recomputed_ambiguous_call_count),
                sum(call_point_count),
                sum(ambiguous_point_count),
                sum(ambiguity_flag_mismatch_count)
            FROM {call_summaries}
            """
        ).fetchone()
        assignment_metrics = connection.execute(
            f"""
            SELECT sum(ambiguous_point_count), sum(assigned_point_count)
            FROM {parquet_sources(assignment_paths)}
            """
        ).fetchone()
        summary = {
            "pipeline_version": __version__,
            "audit_contract_version": AUDIT_CONTRACT_VERSION,
            "source_data_modified": False,
            "years": sorted(config.input.year_directories),
            "definitions": {
                "time_conflict_extent": (
                    "Haversine diagonal of the same-second coordinate bounding box; "
                    "a spatial-separation proxy, not exact pairwise diameter or bound."
                ),
                "ambiguous_point": (
                    f"Nearest and second-nearest port-group distance margin below "
                    f"{current_margin:g} km."
                ),
                "ambiguous_call_rate": (
                    "Calls containing at least one assigned ambiguous point divided "
                    "by all confirmed calls."
                ),
                "unknown_gap_fraction": (
                    "UNKNOWN_GAP duration divided by each vessel-year active span; "
                    "not the share of interval rows."
                ),
            },
            "headline_metrics": {
                "ais_point_count": int(track_metrics[0] or 0),
                "flagged_time_conflict_point_count": int(track_metrics[1] or 0),
                "time_conflict_timestamp_count": int(track_metrics[2] or 0),
                "vessel_active_span_seconds": int(gap_metrics[0] or 0),
                "unknown_gap_seconds": int(gap_metrics[1] or 0),
                "port_call_count": int(call_metrics[0] or 0),
                "recomputed_ambiguous_call_count": int(call_metrics[1] or 0),
                "call_point_count": int(call_metrics[2] or 0),
                "ambiguous_call_point_count": int(call_metrics[3] or 0),
                "ambiguity_flag_mismatch_count": int(call_metrics[4] or 0),
                "all_ambiguous_point_count": int(assignment_metrics[0] or 0),
                "assigned_ambiguous_point_count": int(assignment_metrics[1] or 0),
            },
            "outputs": {
                name: str(path.resolve())
                for name, path in final.items()
                if name != "summary"
            },
            "interpretation_limits": [
                "This audit measures internal consistency and evidence concentration; "
                "it is not a labeled accuracy estimate.",
                "Port-group merges must be reviewed from competing_port_pairs.csv and "
                "review_calls.parquet before changing group_distance_km.",
                "Interval quality flags use bool_or propagation and must not be read as "
                "point-level error rates.",
            ],
        }
        write_json_atomic(temporary["summary"], summary)
        heartbeat(heartbeat_path, "14/14 committing audit reports")
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)

    for name, output in final.items():
        atomic_replace(temporary[name], output)
    heartbeat(heartbeat_path, "committed")
    return {
        "row_count": row_count,
        "output_bytes": sum(path.stat().st_size for path in outputs),
    }


def _require_files(paths: list[Path], stage: str) -> None:
    missing = [path for path in paths if not path.is_file()]
    if not missing:
        return
    preview = "\n".join(f"  {path}" for path in missing[:5])
    suffix = f"\n  ... and {len(missing) - 5} more" if len(missing) > 5 else ""
    raise RuntimeError(
        f"Cannot run quality audit: {stage} inputs are incomplete. "
        f"Run `extractais --config <path> validate` first. Missing:\n"
        f"{preview}{suffix}"
    )


def build_quality_audit(config: AppConfig, store: CheckpointStore) -> None:
    partitions = range(config.layout.track_partitions)
    track_tasks: list[StageTask] = []
    behavior_tasks: list[StageTask] = []
    for partition in partitions:
        track = track_path(config, partition)
        track_outputs = _track_partial_paths(config, partition)
        _require_files([track], "canonical track")
        track_signature = signature(
            [
                AUDIT_CONTRACT_VERSION,
                "track",
                track.stat().st_size,
                track.stat().st_mtime_ns,
            ]
        )
        track_beat = (
            config.storage.temp_root
            / "heartbeats"
            / f"quality-audit-track-{partition:04d}.json"
        )
        track_tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=track_signature,
                source_bytes=max(1, track.stat().st_size),
                outputs=track_outputs,
                heartbeat_path=track_beat,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_track_audit_worker,
                    args=(config, partition, track_outputs, track_beat),
                    resource=str(lane_for_partition(config, partition)),
                ),
            )
        )

        behavior_inputs = [
            *[
                interval_path(config, year, partition)
                for year in sorted(config.input.year_directories)
            ],
            port_call_path(config, partition),
            validation_partial_paths(config, partition)[2],
        ]
        _require_files(behavior_inputs, "validation")
        behavior_outputs = _behavior_partial_paths(config, partition)
        behavior_signature = signature(
            [
                AUDIT_CONTRACT_VERSION,
                "behavior",
                config.ports.ambiguity_margin_km,
                [
                    (path.stat().st_size, path.stat().st_mtime_ns)
                    for path in behavior_inputs
                ],
            ]
        )
        behavior_beat = (
            config.storage.temp_root
            / "heartbeats"
            / f"quality-audit-behavior-{partition:04d}.json"
        )
        behavior_tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=behavior_signature,
                source_bytes=max(1, total_file_size(behavior_inputs)),
                outputs=behavior_outputs,
                heartbeat_path=behavior_beat,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_behavior_audit_worker,
                    args=(config, partition, behavior_outputs, behavior_beat),
                    resource=str(config.storage.products_root),
                ),
            )
        )

    complete = lambda _task, value, _pid, _elapsed: (
        int(value["output_bytes"]),
        int(value["row_count"]),
    )
    run_stage_tasks(
        config,
        store,
        "quality_audit_tracks",
        track_tasks,
        complete,
    )
    run_stage_tasks(
        config,
        store,
        "quality_audit_behavior",
        behavior_tasks,
        complete,
    )

    track_partials = [
        _track_partial_paths(config, partition)
        for partition in range(config.layout.track_partitions)
    ]
    behavior_partials = [
        _behavior_partial_paths(config, partition)
        for partition in range(config.layout.track_partitions)
    ]
    partial_files = [
        path
        for paths in [*track_partials, *behavior_partials]
        for path in paths
    ]
    groups = config.storage.products_root / "ports" / "port_groups.parquet"
    _require_files([*partial_files, groups], "quality-audit partial")
    final = _audit_outputs(config)
    final_outputs = tuple(final.values())
    merge_signature = signature(
        [
            AUDIT_CONTRACT_VERSION,
            "merge",
            [
                (path.stat().st_size, path.stat().st_mtime_ns)
                for path in [*partial_files, groups]
            ],
        ]
    )
    merge_beat = (
        config.storage.temp_root / "heartbeats" / "quality-audit-merge.json"
    )
    run_stage_tasks(
        config,
        store,
        "quality_audit",
        [
            StageTask(
                key="global",
                signature=merge_signature,
                source_bytes=max(1, total_file_size([*partial_files, groups])),
                outputs=final_outputs,
                heartbeat_path=merge_beat,
                call=IsolatedCall(
                    key="global",
                    target=_merge_audit_worker,
                    args=(
                        config,
                        track_partials,
                        behavior_partials,
                        final_outputs,
                        merge_beat,
                    ),
                    resource=str(config.storage.products_root),
                ),
            )
        ],
        complete,
    )
