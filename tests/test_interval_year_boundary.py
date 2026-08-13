from __future__ import annotations

from pathlib import Path

import duckdb

from extractais.calls import group_candidate_path
from extractais.config import load_config
from extractais.intervals import _interval_sql
from extractais.storage import port_call_path, port_context_path, track_path
from test_v2_contract import _config, _ports


def _copy_query(path: Path, query: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (" + query + ") TO ? (FORMAT PARQUET)", [str(path.resolve())]
        )
    finally:
        connection.close()


def _two_year_config(tmp_path: Path):
    raw = tmp_path / "raw"
    (raw / "2021").mkdir(parents=True)
    (raw / "2022").mkdir()
    _ports(raw / "ports.csv")
    config_path = _config(tmp_path, raw)
    text = config_path.read_text(encoding="utf-8")
    config_path.write_text(
        text.replace('    2021: "2021"', '    2021: "2021"\n    2022: "2022"'),
        encoding="utf-8",
    )
    return load_config(config_path)


def test_cross_year_interval_statistics_are_recomputed_per_year(
    tmp_path: Path,
) -> None:
    config = _two_year_config(tmp_path)
    partition = 0
    _copy_query(
        track_path(config, partition),
        """
        SELECT * FROM (VALUES
            (123456789::BIGINT, 1::BIGINT,
             TIMESTAMP '2021-12-31 23:50:00', 10.0::REAL, false, false),
            (123456789::BIGINT, 2::BIGINT,
             TIMESTAMP '2022-01-01 00:10:00', 20.0::REAL, false, true),
            (987654321::BIGINT, 1::BIGINT,
             TIMESTAMP '0021-01-01 00:00:00', 99.0::REAL, false, true)
        ) AS t(mmsi, point_seq, timestamp_utc, speed,
               is_kinematic_outlier, is_time_conflict)
        """,
    )
    _copy_query(
        group_candidate_path(config, partition),
        """
        SELECT
            NULL::BIGINT AS mmsi,
            NULL::BIGINT AS point_seq,
            NULL::VARCHAR AS port_group_id,
            NULL::DOUBLE AS port_distance_km
        WHERE false
        """,
    )
    _copy_query(
        port_context_path(config, partition),
        """
        SELECT
            NULL::BIGINT AS mmsi,
            NULL::BIGINT AS point_seq,
            NULL::DOUBLE AS ambiguity_margin_km
        WHERE false
        """,
    )
    _copy_query(
        port_call_path(config, partition),
        """
        SELECT
            NULL::BIGINT AS mmsi,
            NULL::VARCHAR AS port_call_id,
            NULL::VARCHAR AS port_group_id,
            NULL::VARCHAR AS port_group_name,
            NULL::VARCHAR AS port_country_or_area,
            NULL::TIMESTAMP AS entry_time_utc,
            NULL::TIMESTAMP AS exit_time_utc
        WHERE false
        """,
    )

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            f"""
            SELECT
                year, start_time_utc, end_time_utc,
                ais_point_count, valid_speed_point_count,
                min_speed_knots, mean_speed_knots, max_speed_knots,
                has_time_conflict
            FROM ({_interval_sql(config, partition)})
            ORDER BY year
            """
        ).fetchall()
    finally:
        connection.close()

    assert rows == [
        (
            2021,
            duckdb.execute("SELECT TIMESTAMP '2021-12-31 23:50:00'").fetchone()[0],
            duckdb.execute("SELECT TIMESTAMP '2022-01-01 00:00:00'").fetchone()[0],
            1,
            1,
            10.0,
            10.0,
            10.0,
            False,
        ),
        (
            2022,
            duckdb.execute("SELECT TIMESTAMP '2022-01-01 00:00:00'").fetchone()[0],
            duckdb.execute("SELECT TIMESTAMP '2022-01-01 00:10:00'").fetchone()[0],
            1,
            1,
            20.0,
            20.0,
            20.0,
            True,
        ),
    ]
