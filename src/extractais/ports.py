from __future__ import annotations

import csv
import math
import time
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, Iterable

from tqdm import tqdm

from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql, sql_literal
from extractais.fileutils import (
    remove_path,
    replace_directory,
    temporary_directory,
)
from extractais.gitmeta import git_commit
from extractais.sql import haversine_km, parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)


PORT_SIZE_RANK = {"Very Small": 1, "Small": 2, "Medium": 3, "Large": 4}


def _sql_value(value: Any) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "TRUE" if value else "FALSE"
    if isinstance(value, (int, float)):
        return repr(value)
    return sql_literal(str(value))


def _insert_rows(connection, table: str, rows: list[tuple], batch_size: int) -> None:
    for start in range(0, len(rows), batch_size):
        values = ", ".join(
            "(" + ", ".join(_sql_value(value) for value in row) + ")"
            for row in rows[start : start + batch_size]
        )
        connection.execute(f"INSERT INTO {table} VALUES {values}")


def _float(value: str) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _identifier(value: str, fallback: str) -> str:
    number = _float(value)
    if number is not None and number.is_integer():
        return str(int(number))
    cleaned = (value or "").strip()
    return cleaned or fallback


def _read_ports(config: AppConfig) -> list[Dict[str, Any]]:
    excluded = {value.casefold() for value in config.ports.excluded_harbor_sizes}
    ports: list[Dict[str, Any]] = []
    with config.input.ports_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            latitude = _float(row.get("Latitude", ""))
            longitude = _float(row.get("Longitude", ""))
            if latitude is None or longitude is None:
                continue
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                continue
            harbor_size = (row.get("Harbor Size") or "Unknown").strip() or "Unknown"
            if harbor_size.casefold() in excluded:
                continue
            port_id = _identifier(
                row.get("World Port Index Number", ""), f"ROW-{row_number}"
            )
            ports.append(
                {
                    "port_id": port_id,
                    "main_port_name": (row.get("Main Port Name") or "").strip(),
                    "alternate_port_name": (row.get("Alternate Port Name") or "").strip(),
                    "un_locode": (row.get("UN/LOCODE") or "").strip(),
                    "country": (row.get("Country Code") or "").strip(),
                    "region_name": (row.get("Region Name") or "").strip(),
                    "harbor_size": harbor_size,
                    "harbor_type": (row.get("Harbor Type") or "").strip(),
                    "latitude": latitude,
                    "longitude": longitude,
                }
            )
    if not ports:
        raise ValueError(f"No valid ports found in {config.input.ports_csv}")
    ports.sort(key=lambda row: row["port_id"])
    return ports


def _haversine(left: Dict[str, Any], right: Dict[str, Any]) -> float:
    lat1 = math.radians(left["latitude"])
    lat2 = math.radians(right["latitude"])
    dlat = lat2 - lat1
    dlon = math.radians(right["longitude"] - left["longitude"])
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0088 * 2 * math.asin(math.sqrt(min(1.0, value)))


def _assign_port_groups(
    ports: list[Dict[str, Any]], threshold_km: float, enable_progress: bool
) -> tuple[list[Dict[str, Any]], list[Dict[str, Any]]]:
    parent = list(range(len(ports)))
    spatial_bins: Dict[tuple[int, int], list[int]] = defaultdict(list)
    for index, port in enumerate(ports):
        latitude_cell = math.floor(port["latitude"] + 90.0)
        longitude_cell = math.floor(port["longitude"] + 180.0) % 360
        spatial_bins[(latitude_cell, longitude_cell)].append(index)

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root = find(left)
        right_root = find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    progress = tqdm(
        range(len(ports)),
        desc="group nearby ports",
        disable=not enable_progress,
        dynamic_ncols=True,
    )
    for left_index in progress:
        left = ports[left_index]
        latitude_limit = threshold_km / 111.0
        latitude_cell = math.floor(left["latitude"] + 90.0)
        longitude_cell = math.floor(left["longitude"] + 180.0) % 360
        limiting_latitude = min(89.999, abs(left["latitude"]) + latitude_limit)
        longitude_limit = threshold_km / max(
            0.001, 111.0 * math.cos(math.radians(limiting_latitude))
        )
        longitude_cell_radius = min(180, math.ceil(longitude_limit) + 1)
        candidate_indexes: set[int] = set()
        for candidate_latitude_cell in range(latitude_cell - 1, latitude_cell + 2):
            for offset in range(-longitude_cell_radius, longitude_cell_radius + 1):
                candidate_indexes.update(
                    spatial_bins.get(
                        (candidate_latitude_cell, (longitude_cell + offset) % 360), []
                    )
                )
        for right_index in candidate_indexes:
            if right_index <= left_index:
                continue
            right = ports[right_index]
            if abs(right["latitude"] - left["latitude"]) > latitude_limit:
                continue
            pair_longitude_limit = threshold_km / max(
                0.001,
                111.0
                * math.cos(
                    math.radians((left["latitude"] + right["latitude"]) / 2)
                ),
            )
            longitude_difference = abs(((right["longitude"] - left["longitude"] + 180) % 360) - 180)
            if longitude_difference <= pair_longitude_limit and _haversine(left, right) <= threshold_km:
                union(left_index, right_index)

    members: Dict[int, list[int]] = defaultdict(list)
    for index in range(len(ports)):
        members[find(index)].append(index)

    groups: list[Dict[str, Any]] = []
    for indexes in members.values():
        member_ports = [ports[index] for index in indexes]
        representative = sorted(
            member_ports,
            key=lambda row: (-PORT_SIZE_RANK.get(row["harbor_size"], 0), row["port_id"]),
        )[0]
        group_id = "PG-" + min(row["port_id"] for row in member_ports)
        groups.append(
            {
                "port_group_id": group_id,
                "port_group_name": representative["main_port_name"] or group_id,
                "representative_port_id": representative["port_id"],
                "harbor_size": representative["harbor_size"],
                "member_port_count": len(member_ports),
                "latitude": representative["latitude"],
                "longitude": representative["longitude"],
            }
        )
        for port in member_ports:
            port["port_group_id"] = group_id
            port["port_group_name"] = representative["main_port_name"] or group_id
    groups.sort(key=lambda row: row["port_group_id"])
    return ports, groups


def _write_port_tables(connection, output_root: Path, ports, groups, config: AppConfig) -> None:
    connection.execute("""
        CREATE TEMP TABLE port_catalog (
            port_id VARCHAR, main_port_name VARCHAR, alternate_port_name VARCHAR,
            un_locode VARCHAR, country VARCHAR, region_name VARCHAR, harbor_size VARCHAR,
            harbor_type VARCHAR, latitude DOUBLE, longitude DOUBLE,
            port_group_id VARCHAR, port_group_name VARCHAR
        )
    """)
    _insert_rows(
        connection,
        "port_catalog",
        [
            (
                row["port_id"], row["main_port_name"], row["alternate_port_name"],
                row["un_locode"], row["country"], row["region_name"],
                row["harbor_size"], row["harbor_type"], row["latitude"],
                row["longitude"], row["port_group_id"], row["port_group_name"],
            )
            for row in ports
        ],
        500,
    )
    connection.execute("""
        CREATE TEMP TABLE port_groups (
            port_group_id VARCHAR, port_group_name VARCHAR,
            representative_port_id VARCHAR, harbor_size VARCHAR,
            member_port_count INTEGER, latitude DOUBLE, longitude DOUBLE
        )
    """)
    _insert_rows(
        connection,
        "port_groups",
        [tuple(row.values()) for row in groups],
        500,
    )
    connection.execute(
        parquet_copy_sql(
            "SELECT * FROM port_catalog ORDER BY port_group_id, port_id",
            output_root / "port_catalog.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )
    connection.execute(
        parquet_copy_sql(
            "SELECT * FROM port_groups ORDER BY port_group_id",
            output_root / "port_groups.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )


def _write_anchor_tables(
    connection, output_root: Path, stop_paths: list[Path], config: AppConfig
) -> None:
    stop_source = parquet_sources(stop_paths)
    grid = config.ports.anchor_grid_degrees
    connection.execute(f"""
        CREATE TEMP TABLE anchor_candidates AS
        SELECT
            cast(floor((centroid_latitude + 90.0) / {grid}) AS INTEGER) AS latitude_cell,
            cast(floor((centroid_longitude + 180.0) / {grid}) AS INTEGER) AS longitude_cell,
            avg(centroid_latitude) AS latitude,
            avg(centroid_longitude) AS longitude,
            count(*) AS stop_event_count,
            count(DISTINCT mmsi) AS vessel_count,
            sum(duration_seconds) AS total_stop_seconds,
            sum(source_at_dock_points) AS source_at_dock_points
        FROM {stop_source}
        GROUP BY latitude_cell, longitude_cell
        HAVING count(*) >= {config.ports.anchor_min_stop_events}
           AND count(DISTINCT mmsi) >= {config.ports.anchor_min_vessels}
    """)
    distance = haversine_km("c.latitude", "c.longitude", "p.latitude", "p.longitude")
    assignment_radius = config.ports.anchor_assignment_radius_km
    connection.execute(f"""
        CREATE TEMP TABLE anchor_matches AS
        WITH possible AS (
            SELECT c.*, p.port_id, p.port_group_id, p.port_group_name, {distance} AS center_distance_km
            FROM anchor_candidates c
            JOIN port_catalog p
              ON abs(c.latitude - p.latitude) <= {assignment_radius / 111.0 + 0.01}
             AND abs(((c.longitude - p.longitude + 540.0) % 360.0) - 180.0) <= 1.0
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY latitude_cell, longitude_cell
                ORDER BY center_distance_km, port_id
            ) AS match_rank
            FROM possible
            WHERE center_distance_km <= {assignment_radius}
        )
        SELECT
            md5(concat(latitude_cell::VARCHAR, '|', longitude_cell::VARCHAR, '|', port_group_id))
                AS anchor_id,
            latitude, longitude, stop_event_count, vessel_count, total_stop_seconds,
            source_at_dock_points, port_id AS nearest_port_id, port_group_id,
            port_group_name, center_distance_km
        FROM ranked
        WHERE match_rank = 1
    """)
    connection.execute(
        parquet_copy_sql(
            "SELECT * FROM anchor_matches ORDER BY port_group_id, anchor_id",
            output_root / "anchors.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )
    connection.execute(
        parquet_copy_sql(
            """
            SELECT c.*
            FROM anchor_candidates c
            ANTI JOIN anchor_matches m
              ON c.latitude = m.latitude AND c.longitude = m.longitude
            ORDER BY stop_event_count DESC
            """,
            output_root / "unmatched_anchor_candidates.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )

    stop_distance = haversine_km(
        "s.centroid_latitude", "s.centroid_longitude", "a.latitude", "a.longitude"
    )
    connection.execute(f"""
        CREATE TEMP TABLE stop_port_matches AS
        WITH possible AS (
            SELECT
                s.stop_id, s.mmsi, s.start_time_utc, s.end_time_utc,
                a.anchor_id, a.port_group_id, a.port_group_name,
                {stop_distance} AS anchor_distance_km
            FROM {stop_source} s
            JOIN anchor_matches a
              ON abs(s.centroid_latitude - a.latitude) <= {config.ports.entry_radius_km / 111.0 + 0.01}
             AND abs(((s.centroid_longitude - a.longitude + 540.0) % 360.0) - 180.0) <= 0.25
        ),
        ranked AS (
            SELECT *, row_number() OVER (
                PARTITION BY stop_id ORDER BY anchor_distance_km, anchor_id
            ) AS match_rank
            FROM possible
            WHERE anchor_distance_km <= {config.ports.entry_radius_km}
        )
        SELECT * EXCLUDE (match_rank) FROM ranked WHERE match_rank = 1
    """)
    connection.execute(
        parquet_copy_sql(
            "SELECT * FROM stop_port_matches ORDER BY mmsi, start_time_utc",
            output_root / "stop_port_matches.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )


def _anchor_tile_rows(anchor_rows: Iterable[tuple], radius_km: float) -> list[tuple]:
    rows: list[tuple] = []
    for anchor_id, latitude, longitude in anchor_rows:
        latitude_degrees = radius_km / 111.0 + 0.11
        longitude_degrees = radius_km / max(1.0, 111.0 * math.cos(math.radians(latitude))) + 0.11
        min_latitude_cell = math.floor((max(-90.0, latitude - latitude_degrees) + 90.0) * 10)
        max_latitude_cell = math.floor((min(90.0, latitude + latitude_degrees) + 90.0) * 10)
        min_longitude_cell = math.floor((longitude - longitude_degrees + 180.0) * 10)
        max_longitude_cell = math.floor((longitude + longitude_degrees + 180.0) * 10)
        for latitude_cell in range(min_latitude_cell, max_latitude_cell + 1):
            for raw_longitude_cell in range(min_longitude_cell, max_longitude_cell + 1):
                longitude_cell = raw_longitude_cell % 3600
                rows.append((latitude_cell * 3601 + longitude_cell, anchor_id))
    return rows


def _write_anchor_tiles(connection, output_root: Path, config: AppConfig) -> None:
    anchors = connection.execute(
        "SELECT anchor_id, latitude, longitude FROM anchor_matches"
    ).fetchall()
    connection.execute("CREATE TEMP TABLE anchor_tiles (geo_tile INTEGER, anchor_id VARCHAR)")
    rows = _anchor_tile_rows(anchors, config.ports.approach_radius_km)
    if rows:
        _insert_rows(connection, "anchor_tiles", rows, 5000)
    connection.execute(
        parquet_copy_sql(
            "SELECT DISTINCT geo_tile, anchor_id FROM anchor_tiles ORDER BY geo_tile, anchor_id",
            output_root / "anchor_tiles.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )


def _write_port_coverage(connection, output_root: Path, config: AppConfig) -> None:
    connection.execute(
        parquet_copy_sql(
            """
            SELECT
                g.*,
                count(DISTINCT a.anchor_id) AS anchor_count,
                coalesce(sum(a.stop_event_count), 0) AS supporting_stop_events,
                coalesce(sum(a.vessel_count), 0) AS anchor_vessel_support,
                coalesce(sum(a.source_at_dock_points), 0) AS source_at_dock_points
            FROM port_groups g
            LEFT JOIN anchor_matches a USING (port_group_id)
            GROUP BY ALL
            ORDER BY anchor_count, g.port_group_id
            """,
            output_root / "port_coverage.parquet",
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )


def build_ports(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    stop_paths = sorted(
        (config.storage.work_root / "stage04_stops").glob("mmsi_bucket=*/part.parquet")
    )
    if not stop_paths:
        raise RuntimeError("No stop-event outputs found; run `extractais stops` first")
    source_signature = signature(
        [(str(config.input.ports_csv.resolve()), config.input.ports_csv.stat().st_size, config.input.ports_csv.stat().st_mtime_ns)]
        + [
            (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
            for path in stop_paths
        ]
    )
    stage_hash = config.stage_hash("ports")
    manifest_path = config.storage.work_root / "manifests" / "ports.json"
    manifest = load_stage_manifest(manifest_path, "ports")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
    output_root = config.storage.work_root / "stage05_ports"
    required = [
        output_root / name
        for name in (
            "port_catalog.parquet", "port_groups.parquet", "anchors.parquet",
            "unmatched_anchor_candidates.parquet", "stop_port_matches.parquet",
            "anchor_tiles.parquet", "port_coverage.parquet",
        )
    ]
    if not force and item_is_complete(
        manifest, "catalog", stage_hash, source_signature, required
    ):
        return manifest

    started = time.perf_counter()
    ports = _read_ports(config)
    ports, groups = _assign_port_groups(
        ports, config.ports.group_distance_km, config.runtime.enable_progress
    )
    temporary = temporary_directory(output_root)
    remove_path(temporary, config.storage.work_root)
    temporary.mkdir(parents=True, exist_ok=True)
    connection = open_database(config)
    try:
        _write_port_tables(connection, temporary, ports, groups, config)
        _write_anchor_tables(connection, temporary, stop_paths, config)
        _write_anchor_tiles(connection, temporary, config)
        _write_port_coverage(connection, temporary, config)
    finally:
        connection.close()
    replace_directory(temporary, output_root, config.storage.work_root)
    manifest["items"]["catalog"] = {
        "status": "complete",
        "config_hash": stage_hash,
        "source_signature": source_signature,
        "output": str(output_root.resolve()),
        "port_count": len(ports),
        "port_group_count": len(groups),
        "elapsed_seconds": round(time.perf_counter() - started, 3),
        "completed_at_utc": utc_now(),
    }
    save_stage_manifest(manifest_path, manifest)
    return manifest
