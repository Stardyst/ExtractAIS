from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Dict

from tqdm import tqdm

from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
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
from extractais.stops import bucket_number, track_bucket_files


def _write_candidates(connection, config: AppConfig, track_path: Path, output: Path) -> None:
    ports_root = config.storage.work_root / "stage05_ports"
    track_source = parquet_sources([track_path])
    tile_source = parquet_sources([ports_root / "anchor_tiles.parquet"])
    anchor_source = parquet_sources([ports_root / "anchors.parquet"])
    distance = haversine_km("t.latitude", "t.longitude", "a.latitude", "a.longitude")
    select_sql = f"""
        WITH raw_candidates AS (
            SELECT
                t.mmsi, t.point_seq, t.timestamp_utc, t.latitude, t.longitude,
                t.speed, t.source_at_dock, t.gap_seconds, t.mmsi_bucket,
                t.matched_port_name, t.source_label, t.source_sublabel,
                t.collection_type, t.source,
                a.anchor_id, a.port_group_id, a.port_group_name,
                {distance} AS anchor_distance_km
            FROM {track_source} t
            JOIN {tile_source} z USING (geo_tile)
            JOIN {anchor_source} a USING (anchor_id)
            WHERE NOT t.is_kinematic_outlier
        ),
        within_approach AS (
            SELECT * FROM raw_candidates
            WHERE anchor_distance_km <= {config.ports.approach_radius_km}
        ),
        by_group AS (
            SELECT
                mmsi, point_seq, timestamp_utc, latitude, longitude, speed,
                source_at_dock, gap_seconds, mmsi_bucket,
                matched_port_name, source_label, source_sublabel,
                collection_type, source,
                port_group_id,
                arg_min(port_group_name, anchor_distance_km) AS port_group_name,
                arg_min(anchor_id, anchor_distance_km) AS nearest_anchor_id,
                min(anchor_distance_km) AS port_distance_km
            FROM within_approach
            GROUP BY mmsi, point_seq, timestamp_utc, latitude, longitude, speed,
                     source_at_dock, gap_seconds, mmsi_bucket,
                     matched_port_name, source_label, source_sublabel,
                     collection_type, source, port_group_id
        )
        SELECT
            *,
            row_number() OVER (
                PARTITION BY mmsi, point_seq
                ORDER BY port_distance_km, port_group_id
            ) AS candidate_rank
        FROM by_group
        ORDER BY mmsi, point_seq, candidate_rank
    """
    connection.execute(
        parquet_copy_sql(
            select_sql,
            output,
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )


def _write_port_calls(
    connection, config: AppConfig, candidates_path: Path, output: Path
) -> None:
    candidates = parquet_sources([candidates_path])
    stop_matches = parquet_sources(
        [config.storage.work_root / "stage05_ports" / "stop_port_matches.parquet"]
    )
    maximum_gap_seconds = int(config.ports.call_max_point_gap_hours * 3600)
    select_sql = f"""
        WITH best AS (
            SELECT
                first.*,
                second.port_distance_km AS second_port_distance_km,
                second.port_distance_km - first.port_distance_km AS ambiguity_margin_km
            FROM {candidates} first
            LEFT JOIN {candidates} second
              ON first.mmsi = second.mmsi
             AND first.point_seq = second.point_seq
             AND second.candidate_rank = 2
            WHERE first.candidate_rank = 1
        ),
        previous AS (
            SELECT
                *,
                lag(point_seq) OVER (PARTITION BY mmsi ORDER BY point_seq) AS previous_point_seq,
                lag(port_group_id) OVER (PARTITION BY mmsi ORDER BY point_seq) AS previous_port_group_id,
                lag(timestamp_utc) OVER (PARTITION BY mmsi ORDER BY point_seq) AS previous_time_utc
            FROM best
        ),
        marked AS (
            SELECT
                *,
                CASE WHEN previous_point_seq IS NULL
                       OR point_seq <> previous_point_seq + 1
                       OR port_group_id <> previous_port_group_id
                       OR date_diff('second', previous_time_utc, timestamp_utc) > {maximum_gap_seconds}
                     THEN 1 ELSE 0 END AS new_episode
            FROM previous
        ),
        grouped AS (
            SELECT
                *,
                sum(new_episode) OVER (
                    PARTITION BY mmsi ORDER BY point_seq ROWS UNBOUNDED PRECEDING
                ) AS episode_number
            FROM marked
        ),
        episodes AS (
            SELECT
                mmsi, mmsi_bucket, episode_number, port_group_id,
                arg_min(port_group_name, timestamp_utc) AS port_group_name,
                min(timestamp_utc) AS approach_start_time_utc,
                max(timestamp_utc) AS approach_end_time_utc,
                min(timestamp_utc) FILTER (
                    WHERE port_distance_km <= {config.ports.entry_radius_km}
                ) AS entry_time_utc,
                max(timestamp_utc) FILTER (
                    WHERE port_distance_km <= {config.ports.exit_radius_km}
                ) AS exit_time_utc,
                count(*) AS point_count,
                min(port_distance_km) AS minimum_port_distance_km,
                min(speed) AS minimum_speed_knots,
                count(*) FILTER (WHERE source_at_dock) AS source_at_dock_points,
                count(*) FILTER (WHERE matched_port_name IS NOT NULL)
                    AS source_matched_port_points,
                min(ambiguity_margin_km) AS minimum_ambiguity_margin_km
            FROM grouped
            GROUP BY mmsi, mmsi_bucket, episode_number, port_group_id
        ),
        evidence AS (
            SELECT
                e.*,
                count(DISTINCT s.stop_id) AS matched_stop_count
            FROM episodes e
            LEFT JOIN {stop_matches} s
              ON e.mmsi = s.mmsi
             AND e.port_group_id = s.port_group_id
             AND s.end_time_utc >= e.approach_start_time_utc
             AND s.start_time_utc <= e.approach_end_time_utc
            GROUP BY ALL
        )
        SELECT
            md5(concat(mmsi::VARCHAR, '|', port_group_id, '|', entry_time_utc::VARCHAR))
                AS port_call_id,
            *
        FROM evidence
        WHERE entry_time_utc IS NOT NULL
          AND exit_time_utc IS NOT NULL
          AND point_count >= {config.ports.call_min_points}
          AND matched_stop_count > 0
        ORDER BY mmsi, entry_time_utc
    """
    connection.execute(
        parquet_copy_sql(
            select_sql,
            output,
            config.prepare.compression,
            config.prepare.row_group_size,
        )
    )


def build_port_calls(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    tracks = track_bucket_files(config)
    ports_root = config.storage.work_root / "stage05_ports"
    dependencies = [
        ports_root / "anchors.parquet",
        ports_root / "anchor_tiles.parquet",
        ports_root / "stop_port_matches.parquet",
    ]
    if not all(path.exists() for path in dependencies):
        raise RuntimeError("Port artifacts are incomplete; run `extractais ports` first")

    manifest_path = config.storage.work_root / "manifests" / "calls.json"
    manifest = load_stage_manifest(manifest_path, "calls")
    stage_hash = config.stage_hash("ports", "prepare")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
    dependency_identity = [
        (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
        for path in dependencies
    ]
    output_root = config.storage.work_root / "stage06_port_calls"
    progress = tqdm(
        tracks,
        desc="recognize port calls",
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
    )
    for track_path in progress:
        bucket = bucket_number(track_path)
        source_signature = signature(
            [(str(track_path.resolve()), track_path.stat().st_size, track_path.stat().st_mtime_ns)]
            + dependency_identity
        )
        bucket_root = output_root / f"mmsi_bucket={bucket:04d}"
        candidates_output = bucket_root / "candidates.parquet"
        calls_output = bucket_root / "port_calls.parquet"
        key = f"bucket:{bucket:04d}"
        if not force and item_is_complete(
            manifest,
            key,
            stage_hash,
            source_signature,
            [candidates_output, calls_output],
        ):
            continue

        started = time.perf_counter()
        temporary_root = temporary_directory(bucket_root)
        remove_path(temporary_root, config.storage.work_root)
        temporary_root.mkdir(parents=True, exist_ok=True)
        temporary_candidates = temporary_root / "candidates.parquet"
        temporary_calls = temporary_root / "port_calls.parquet"
        connection = open_database(config)
        try:
            _write_candidates(connection, config, track_path, temporary_candidates)
            _write_port_calls(connection, config, temporary_candidates, temporary_calls)
            candidate_count = int(
                connection.execute(
                    f"SELECT count(*) FROM {parquet_sources([temporary_candidates])}"
                ).fetchone()[0]
            )
            call_count = int(
                connection.execute(
                    f"SELECT count(*) FROM {parquet_sources([temporary_calls])}"
                ).fetchone()[0]
            )
        finally:
            connection.close()
        replace_directory(temporary_root, bucket_root, config.storage.work_root)
        manifest["items"][key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": source_signature,
            "output": str(bucket_root.resolve()),
            "candidate_count": candidate_count,
            "port_call_count": call_count,
            "elapsed_seconds": round(time.perf_counter() - started, 3),
            "completed_at_utc": utc_now(),
        }
        save_stage_manifest(manifest_path, manifest)
    return manifest
