from __future__ import annotations

import csv
import math
import shutil
from collections import defaultdict
from pathlib import Path
from typing import Any

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.isolated import IsolatedCall
from extractais.runtime import StageTask, atomic_replace, heartbeat, run_stage_tasks, signature
from extractais.sql import haversine_km, parquet_sources
from extractais.storage import (
    ensure_space,
    shared_temp_requirement,
    stop_path,
    total_file_size,
)


PORT_SIZE_RANK = {"Very Small": 1, "Small": 2, "Medium": 3, "Large": 4}


def _number(value: str) -> float | None:
    try:
        result = float(value)
    except (TypeError, ValueError):
        return None
    return result if math.isfinite(result) else None


def _identifier(value: str, fallback: str) -> str:
    number = _number(value)
    if number is not None and number.is_integer():
        return str(int(number))
    return (value or "").strip() or fallback


def read_ports(
    config: AppConfig, *, apply_exclusions: bool = True
) -> list[dict[str, Any]]:
    excluded = (
        {value.casefold() for value in config.ports.excluded_harbor_sizes}
        if apply_exclusions
        else set()
    )
    result: list[dict[str, Any]] = []
    with config.input.ports_csv.open("r", encoding="utf-8-sig", newline="") as handle:
        for row_number, row in enumerate(csv.DictReader(handle), start=2):
            latitude = _number(row.get("Latitude", ""))
            longitude = _number(row.get("Longitude", ""))
            if latitude is None or longitude is None:
                continue
            if not (-90 <= latitude <= 90 and -180 <= longitude <= 180):
                continue
            harbor_size = (row.get("Harbor Size") or "Unknown").strip() or "Unknown"
            if harbor_size.casefold() in excluded:
                continue
            result.append(
                {
                    "port_id": _identifier(
                        row.get("World Port Index Number", ""), f"ROW-{row_number}"
                    ),
                    "main_port_name": (row.get("Main Port Name") or "").strip(),
                    "alternate_port_name": (row.get("Alternate Port Name") or "").strip(),
                    "un_locode": (row.get("UN/LOCODE") or "").strip(),
                    "country_or_area_name": (
                        row.get("Country Name") or row.get("Country Code") or ""
                    ).strip(),
                    "region_name": (row.get("Region Name") or "").strip(),
                    "harbor_size": harbor_size,
                    "harbor_type": (row.get("Harbor Type") or "").strip(),
                    "latitude": latitude,
                    "longitude": longitude,
                }
            )
    if not result:
        raise ValueError(f"No valid ports found in {config.input.ports_csv}")
    return sorted(result, key=lambda row: row["port_id"])


def _distance(left: dict[str, Any], right: dict[str, Any]) -> float:
    lat1 = math.radians(left["latitude"])
    lat2 = math.radians(right["latitude"])
    dlat = lat2 - lat1
    dlon = math.radians(right["longitude"] - left["longitude"])
    value = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 6371.0088 * 2 * math.asin(math.sqrt(min(1.0, value)))


def assign_port_groups(
    ports: list[dict[str, Any]], threshold_km: float
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    parent = list(range(len(ports)))
    bins: dict[tuple[int, int], list[int]] = defaultdict(list)
    for index, port in enumerate(ports):
        bins[(math.floor(port["latitude"] + 90), math.floor(port["longitude"] + 180) % 360)].append(index)

    def find(index: int) -> int:
        while parent[index] != index:
            parent[index] = parent[parent[index]]
            index = parent[index]
        return index

    def union(left: int, right: int) -> None:
        left_root, right_root = find(left), find(right)
        if left_root != right_root:
            parent[max(left_root, right_root)] = min(left_root, right_root)

    for left_index, left in enumerate(ports):
        lat_cell = math.floor(left["latitude"] + 90)
        lon_cell = math.floor(left["longitude"] + 180) % 360
        latitude_limit = threshold_km / 111.0
        longitude_radius = min(
            180,
            math.ceil(
                threshold_km
                / max(0.001, 111 * math.cos(math.radians(min(89.999, abs(left["latitude"]) + latitude_limit))))
            )
            + 1,
        )
        candidates: set[int] = set()
        for candidate_lat in range(lat_cell - 1, lat_cell + 2):
            for offset in range(-longitude_radius, longitude_radius + 1):
                candidates.update(bins.get((candidate_lat, (lon_cell + offset) % 360), []))
        for right_index in candidates:
            if right_index > left_index and _distance(left, ports[right_index]) <= threshold_km:
                union(left_index, right_index)

    members: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, port in enumerate(ports):
        members[find(index)].append(port)
    groups: list[dict[str, Any]] = []
    for member_ports in members.values():
        representative = sorted(
            member_ports,
            key=lambda row: (-PORT_SIZE_RANK.get(row["harbor_size"], 0), row["port_id"]),
        )[0]
        group_id = "PG-" + min(row["port_id"] for row in member_ports)
        countries = sorted(
            {row["country_or_area_name"] for row in member_ports if row["country_or_area_name"]}
        )
        group_name = representative["main_port_name"] or group_id
        groups.append(
            {
                "port_group_id": group_id,
                "port_group_name": group_name,
                "representative_port_id": representative["port_id"],
                "country_or_area_name": "; ".join(countries),
                "member_country_or_area_names": countries,
                "member_country_or_area_count": len(countries),
                "is_cross_border": len(countries) > 1,
                "harbor_size": representative["harbor_size"],
                "member_port_count": len(member_ports),
                "latitude": representative["latitude"],
                "longitude": representative["longitude"],
            }
        )
        for port in member_ports:
            port["port_group_id"] = group_id
            port["port_group_name"] = group_name
    return ports, sorted(groups, key=lambda row: row["port_group_id"])


def anchor_cell_path(config: AppConfig, partition: int) -> Path:
    return config.storage.products_root / "ports" / "anchor_cells" / f"partition={partition:04d}.parquet"


def _anchor_cell_worker(
    config: AppConfig,
    partition: int,
    output: Path,
    heartbeat_path: Path,
) -> dict[str, int]:
    source_path = stop_path(config, partition)
    temporary = output.with_name(output.stem + ".tmp" + output.suffix)
    worker_temp = config.storage.temp_root / f"anchor-cells-{partition:04d}"
    temporary.unlink(missing_ok=True)
    shutil.rmtree(worker_temp, ignore_errors=True)
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        max(source_path.stat().st_size, 1024**2),
        f"anchor cells {partition:04d}",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        f"anchor cells temporary {partition:04d}",
    )
    heartbeat(heartbeat_path, "aggregating stop cells")
    connection = open_database(config, worker_temp, worker=True)
    try:
        grid = config.ports.anchor_grid_degrees
        count = int(
            connection.execute(
                parquet_copy_sql(
                    f"""
                    SELECT
                        cast(floor((centroid_latitude + 90.0) / {grid}) AS INTEGER)
                            AS latitude_cell,
                        cast(floor((centroid_longitude + 180.0) / {grid}) AS INTEGER)
                            AS longitude_cell,
                        sum(centroid_latitude * point_count) AS weighted_latitude,
                        sum(centroid_longitude * point_count) AS weighted_longitude,
                        sum(point_count) AS position_weight,
                        count(*) AS stop_event_count,
                        count(DISTINCT mmsi) AS vessel_count,
                        sum(duration_seconds) AS total_stop_seconds,
                        sum(source_at_dock_points) AS source_at_dock_points
                    FROM {parquet_sources([source_path])}
                    GROUP BY latitude_cell, longitude_cell
                    """,
                    temporary,
                    config,
                    order_by="latitude_cell, longitude_cell",
                )
            ).fetchone()[0]
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)
    atomic_replace(temporary, output)
    heartbeat(heartbeat_path, "committed")
    return {"row_count": count, "output_bytes": output.stat().st_size}


def _insert_ports(connection, ports: list[dict[str, Any]]) -> None:
    connection.execute(
        """
        CREATE TEMP TABLE port_members (
            port_id VARCHAR, main_port_name VARCHAR, alternate_port_name VARCHAR,
            un_locode VARCHAR, country_or_area_name VARCHAR, region_name VARCHAR,
            harbor_size VARCHAR, harbor_type VARCHAR, latitude DOUBLE, longitude DOUBLE
        )
        """
    )
    connection.executemany(
        "INSERT INTO port_members VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [tuple(row.values()) for row in ports],
    )


def _anchor_catalog_worker(
    config: AppConfig,
    cell_paths: list[Path],
    outputs: tuple[Path, ...],
    heartbeat_path: Path,
) -> dict[str, int]:
    ports = read_ports(config, apply_exclusions=False)
    temporary_outputs = tuple(path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs)
    worker_temp = config.storage.temp_root / "anchor-catalog"
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        "port anchors temporary data",
    )
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(config, worker_temp, worker=False)
    try:
        heartbeat(heartbeat_path, "merging anchor cells")
        _insert_ports(connection, ports)
        connection.execute(
            f"""
            CREATE TEMP TABLE anchor_candidates AS
            SELECT
                latitude_cell,
                longitude_cell,
                sum(weighted_latitude) / sum(position_weight) AS latitude,
                sum(weighted_longitude) / sum(position_weight) AS longitude,
                sum(stop_event_count)::BIGINT AS stop_event_count,
                sum(vessel_count)::BIGINT AS vessel_count,
                sum(total_stop_seconds)::BIGINT AS total_stop_seconds,
                sum(source_at_dock_points)::BIGINT AS source_at_dock_points
            FROM {parquet_sources(cell_paths)}
            GROUP BY latitude_cell, longitude_cell
            HAVING sum(stop_event_count) >= {config.ports.anchor_min_stop_events}
               AND sum(vessel_count) >= {config.ports.anchor_min_vessels}
            """
        )
        distance = haversine_km("a.latitude", "a.longitude", "p.latitude", "p.longitude")
        radius = config.ports.anchor_assignment_radius_km
        heartbeat(heartbeat_path, "matching anchors to WPI ports")
        connection.execute(
            f"""
            CREATE TEMP TABLE anchor_port_candidates AS
            SELECT
                md5(concat(a.latitude_cell::VARCHAR, '|', a.longitude_cell::VARCHAR))
                    AS anchor_id,
                a.*,
                p.port_id,
                {distance} AS center_distance_km
            FROM anchor_candidates a
            JOIN port_members p
              ON abs(a.latitude - p.latitude) <= {radius / 111.0 + 0.01}
             AND abs(((a.longitude - p.longitude + 540.0) % 360.0) - 180.0) <= 1.0
            WHERE {distance} <= {radius}
            """
        )
        connection.execute(
            """
            CREATE TEMP TABLE anchors AS
            SELECT * EXCLUDE (port_id, center_distance_km, match_rank),
                   port_id AS nearest_port_id,
                   center_distance_km AS nearest_port_distance_km
            FROM (
                SELECT *, row_number() OVER (
                    PARTITION BY anchor_id ORDER BY center_distance_km, port_id
                ) AS match_rank
                FROM anchor_port_candidates
            )
            WHERE match_rank = 1
            """
        )
        copy_specs = [
            ("SELECT * FROM anchors", temporary_outputs[0], "anchor_id"),
            (
                "SELECT * FROM anchor_port_candidates",
                temporary_outputs[1],
                "anchor_id, center_distance_km, port_id",
            ),
            (
                """
                SELECT a.*
                FROM anchor_candidates a
                ANTI JOIN anchors m USING (latitude_cell, longitude_cell)
                """,
                temporary_outputs[2],
                "stop_event_count DESC, latitude_cell, longitude_cell",
            ),
        ]
        counts = []
        for query, output, order in copy_specs:
            counts.append(int(connection.execute(parquet_copy_sql(query, output, config, order_by=order)).fetchone()[0]))

        heartbeat(heartbeat_path, "building anchor tile index")
        anchors = connection.execute("SELECT anchor_id, latitude, longitude FROM anchors").fetchall()
        connection.execute("CREATE TEMP TABLE anchor_tiles (geo_tile INTEGER, anchor_id VARCHAR)")
        tile_rows: list[tuple[int, str]] = []
        for anchor_id, latitude, longitude in anchors:
            lat_degrees = config.ports.approach_radius_km / 111.0 + 0.11
            lon_degrees = config.ports.approach_radius_km / max(
                1.0, 111.0 * math.cos(math.radians(latitude))
            ) + 0.11
            for lat_cell in range(
                math.floor((max(-90.0, latitude - lat_degrees) + 90.0) * 10),
                math.floor((min(90.0, latitude + lat_degrees) + 90.0) * 10) + 1,
            ):
                for raw_lon in range(
                    math.floor((longitude - lon_degrees + 180.0) * 10),
                    math.floor((longitude + lon_degrees + 180.0) * 10) + 1,
                ):
                    tile_rows.append((lat_cell * 3601 + raw_lon % 3600, anchor_id))
        if tile_rows:
            connection.executemany("INSERT INTO anchor_tiles VALUES (?, ?)", tile_rows)
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        "SELECT DISTINCT * FROM anchor_tiles",
                        temporary_outputs[3],
                        config,
                        order_by="geo_tile, anchor_id",
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
    return {"row_count": sum(counts), "output_bytes": sum(path.stat().st_size for path in outputs)}


def _group_worker(
    config: AppConfig,
    outputs: tuple[Path, Path, Path],
    heartbeat_path: Path,
) -> dict[str, int]:
    ports, groups = assign_port_groups(read_ports(config), config.ports.group_distance_km)
    temporary_outputs = tuple(path.with_name(path.stem + ".tmp" + path.suffix) for path in outputs)
    worker_temp = config.storage.temp_root / "port-groups"
    ensure_space(
        config.storage.products_root,
        config.storage.reserves_gib["products"],
        max(config.input.ports_csv.stat().st_size * 2, 1024**2),
        "port group catalog",
    )
    ensure_space(
        config.storage.temp_root,
        config.storage.reserves_gib["temp"],
        shared_temp_requirement(config),
        "port groups temporary data",
    )
    shutil.rmtree(worker_temp, ignore_errors=True)
    for path in temporary_outputs:
        path.unlink(missing_ok=True)
        path.parent.mkdir(parents=True, exist_ok=True)
    connection = open_database(config, worker_temp, worker=False)
    try:
        heartbeat(heartbeat_path, "grouping nearby WPI ports")
        connection.execute(
            """
            CREATE TEMP TABLE port_catalog (
                port_id VARCHAR, main_port_name VARCHAR, alternate_port_name VARCHAR,
                un_locode VARCHAR, country_or_area_name VARCHAR, region_name VARCHAR,
                harbor_size VARCHAR, harbor_type VARCHAR, latitude DOUBLE, longitude DOUBLE,
                port_group_id VARCHAR, port_group_name VARCHAR
            )
            """
        )
        connection.executemany(
            "INSERT INTO port_catalog VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(row.values()) for row in ports],
        )
        connection.execute(
            """
            CREATE TEMP TABLE port_groups (
                port_group_id VARCHAR, port_group_name VARCHAR,
                representative_port_id VARCHAR, country_or_area_name VARCHAR,
                member_country_or_area_names VARCHAR[],
                member_country_or_area_count INTEGER, is_cross_border BOOLEAN,
                harbor_size VARCHAR, member_port_count INTEGER,
                latitude DOUBLE, longitude DOUBLE
            )
            """
        )
        connection.executemany(
            "INSERT INTO port_groups VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [tuple(row.values()) for row in groups],
        )
        counts = []
        counts.append(int(connection.execute(parquet_copy_sql("SELECT * FROM port_catalog", temporary_outputs[0], config, order_by="port_group_id, port_id")).fetchone()[0]))
        counts.append(int(connection.execute(parquet_copy_sql("SELECT * FROM port_groups", temporary_outputs[1], config, order_by="port_group_id")).fetchone()[0]))
        anchors = parquet_sources([config.storage.products_root / "ports" / "anchors.parquet"])
        counts.append(
            int(
                connection.execute(
                    parquet_copy_sql(
                        f"""
                        SELECT
                            g.*,
                            count(DISTINCT a.anchor_id) AS anchor_count,
                            coalesce(sum(a.stop_event_count), 0) AS supporting_stop_events,
                            coalesce(sum(a.vessel_count), 0) AS anchor_vessel_support
                        FROM port_groups g
                        LEFT JOIN port_catalog p USING (port_group_id)
                        LEFT JOIN {anchors} a ON a.nearest_port_id = p.port_id
                        GROUP BY ALL
                        """,
                        temporary_outputs[2],
                        config,
                        order_by="port_group_id",
                    )
                ).fetchone()[0]
            )
        )
    finally:
        connection.close()
        shutil.rmtree(worker_temp, ignore_errors=True)
    for temporary, output in zip(temporary_outputs, outputs):
        atomic_replace(temporary, output)
    heartbeat(heartbeat_path, "committed")
    return {"row_count": sum(counts), "output_bytes": sum(path.stat().st_size for path in outputs)}


def build_ports(config: AppConfig, store: CheckpointStore) -> None:
    root = config.storage.products_root / "ports"
    cell_tasks: list[StageTask] = []
    cell_hash = signature([
        config.raw["ports"]["anchor_grid_degrees"],
        config.layout.track_partitions,
    ])
    for partition in range(config.layout.track_partitions):
        source = stop_path(config, partition)
        output = anchor_cell_path(config, partition)
        task_signature = signature([cell_hash, source.stat().st_size, source.stat().st_mtime_ns])
        beat = config.storage.temp_root / "heartbeats" / f"anchor-cells-{partition:04d}.json"
        cell_tasks.append(
            StageTask(
                key=f"{partition:04d}",
                signature=task_signature,
                source_bytes=max(1, source.stat().st_size),
                outputs=(output,),
                heartbeat_path=beat,
                call=IsolatedCall(
                    key=f"{partition:04d}",
                    target=_anchor_cell_worker,
                    args=(config, partition, output, beat),
                    resource=str(source.anchor),
                ),
            )
        )

    run_stage_tasks(
        config,
        store,
        "anchor_cells",
        cell_tasks,
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )

    cell_paths = [anchor_cell_path(config, partition) for partition in range(config.layout.track_partitions)]
    anchor_outputs = tuple(
        root / name
        for name in (
            "anchors.parquet",
            "anchor_port_candidates.parquet",
            "unmatched_anchor_candidates.parquet",
            "anchor_tiles.parquet",
        )
    )
    anchor_signature = signature(
        [
            config.input.ports_csv.stat().st_size,
            config.input.ports_csv.stat().st_mtime_ns,
            config.ports.anchor_assignment_radius_km,
            config.ports.anchor_min_stop_events,
            config.ports.anchor_min_vessels,
            config.ports.approach_radius_km,
            [(path.stat().st_size, path.stat().st_mtime_ns) for path in cell_paths],
        ]
    )
    if not store.is_complete("anchors", "global", anchor_signature, anchor_outputs):
        ensure_space(
            root,
            config.storage.reserves_gib["products"],
            total_file_size(cell_paths) * 2,
            "port anchors",
        )
    anchor_beat = config.storage.temp_root / "heartbeats" / "anchors-global.json"
    run_stage_tasks(
        config,
        store,
        "anchors",
        [
            StageTask(
                key="global",
                signature=anchor_signature,
                source_bytes=max(1, total_file_size(cell_paths)),
                outputs=anchor_outputs,
                heartbeat_path=anchor_beat,
                call=IsolatedCall(
                    key="global",
                    target=_anchor_catalog_worker,
                    args=(config, cell_paths, anchor_outputs, anchor_beat),
                    resource="port-products",
                ),
            )
        ],
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )

    group_outputs = tuple(root / name for name in ("port_catalog.parquet", "port_groups.parquet", "port_coverage.parquet"))
    group_signature = signature(
        [
            config.input.ports_csv.stat().st_size,
            config.input.ports_csv.stat().st_mtime_ns,
            config.ports.group_distance_km,
            config.ports.excluded_harbor_sizes,
            anchor_outputs[0].stat().st_size,
        ]
    )
    group_beat = config.storage.temp_root / "heartbeats" / "port-groups.json"
    run_stage_tasks(
        config,
        store,
        "port_groups",
        [
            StageTask(
                key="global",
                signature=group_signature,
                source_bytes=max(1, config.input.ports_csv.stat().st_size),
                outputs=group_outputs,
                heartbeat_path=group_beat,
                call=IsolatedCall(
                    key="global",
                    target=_group_worker,
                    args=(config, group_outputs, group_beat),
                    resource="port-products",
                ),
            )
        ],
        lambda _task, value, _pid, _elapsed: (
            int(value["output_bytes"]), int(value["row_count"])
        ),
    )
