from __future__ import annotations

from pathlib import Path

import duckdb

from extractais.config import load_config
from extractais.geometry import _geometry_worker, stop_anchor_match_path
from extractais.storage import candidate_path, stop_path, track_path
from test_v2_contract import _config, _ports


def _copy(connection: duckdb.DuckDBPyConnection, sql: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection.execute(
        f"COPY ({sql}) TO '{path.as_posix()}' (FORMAT PARQUET, COMPRESSION ZSTD)"
    )


def test_geometry_keeps_only_nearest_anchor_per_point_and_wpi_port(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _ports(raw / "ports.csv")
    config = load_config(_config(tmp_path, raw))
    track = track_path(config, 0)
    stops = stop_path(config, 0)
    ports_root = config.storage.products_root / "ports"
    anchors = ports_root / "anchors.parquet"
    tiles = ports_root / "anchor_tiles.parquet"

    connection = duckdb.connect()
    try:
        _copy(
            connection,
            """
            SELECT
                123456789::BIGINT AS mmsi,
                7::BIGINT AS point_seq,
                timestamp '2021-01-01 00:00:00' AS timestamp_utc,
                0.0::DOUBLE AS latitude,
                0.0::DOUBLE AS longitude,
                2.0::DOUBLE AS speed,
                false AS source_at_dock,
                NULL::BIGINT AS gap_seconds,
                NULL::VARCHAR AS matched_port_name,
                NULL::VARCHAR AS source_label,
                NULL::VARCHAR AS source_sublabel,
                NULL::VARCHAR AS collection_type,
                NULL::VARCHAR AS source,
                0::INTEGER AS track_partition_id,
                3240900::INTEGER AS geo_tile,
                false AS is_kinematic_outlier
            """,
            track,
        )
        _copy(
            connection,
            """
            SELECT
                NULL::VARCHAR AS stop_id,
                NULL::BIGINT AS mmsi,
                NULL::TIMESTAMP AS start_time_utc,
                NULL::TIMESTAMP AS end_time_utc,
                NULL::INTEGER AS track_partition_id,
                NULL::DOUBLE AS centroid_latitude,
                NULL::DOUBLE AS centroid_longitude
            WHERE false
            """,
            stops,
        )
        _copy(
            connection,
            """
            SELECT * FROM (VALUES
                ('A-near', 0.001::DOUBLE, 0.0::DOUBLE, '1'),
                ('A-far',  0.010::DOUBLE, 0.0::DOUBLE, '1'),
                ('B-near', 0.002::DOUBLE, 0.0::DOUBLE, '2')
            ) AS a(anchor_id, latitude, longitude, nearest_port_id)
            """,
            anchors,
        )
        _copy(
            connection,
            """
            SELECT * FROM (VALUES
                (3240900::INTEGER, 'A-near'),
                (3240900::INTEGER, 'A-far'),
                (3240900::INTEGER, 'B-near')
            ) AS z(geo_tile, anchor_id)
            """,
            tiles,
        )
    finally:
        connection.close()

    outputs = (
        candidate_path(config, 0),
        stop_anchor_match_path(config, 0),
    )
    heartbeat = config.storage.temp_root / "heartbeats" / "geometry-0000.json"
    _geometry_worker(config, 0, outputs, heartbeat)

    result = duckdb.read_parquet(str(outputs[0]))
    assert result.columns == [
        "mmsi",
        "point_seq",
        "timestamp_utc",
        "track_partition_id",
        "nearest_port_id",
        "nearest_anchor_id",
        "anchor_distance_km",
    ]
    rows = result.order("nearest_port_id").fetchall()
    assert len(rows) == 2
    assert rows[0][4:6] == ("1", "A-near")
    assert rows[1][4:6] == ("2", "B-near")
    assert rows[0][6] < rows[1][6] < 0.3
