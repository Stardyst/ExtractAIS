from __future__ import annotations

from pathlib import Path
from typing import Any, Dict

from extractais.bucketstage import BucketExecution, BucketTask, run_bucket_stage
from extractais.config import AppConfig
from extractais.database import open_database, parquet_copy_sql
from extractais.fileutils import (
    remove_path,
    replace_directory,
    temporary_directory,
)
from extractais.gitmeta import git_commit
from extractais.isolated import IsolatedCall
from extractais.sql import haversine_km, parquet_sources
from extractais.stage import (
    item_is_complete,
    load_stage_manifest,
    save_stage_manifest,
    signature,
    utc_now,
)
from extractais.storage import directory_size, free_space_bytes
from extractais.stops import bucket_number, track_bucket_files


def _write_candidates(connection, config: AppConfig, track_path: Path, output: Path) -> int:
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


def _write_port_calls(
    connection,
    config: AppConfig,
    context_path: Path,
    stop_matches_path: Path,
    output: Path,
) -> int:
    context = parquet_sources([context_path])
    stop_matches = parquet_sources([stop_matches_path])
    maximum_gap_seconds = int(config.ports.call_max_point_gap_hours * 3600)
    select_sql = f"""
        WITH previous AS (
            SELECT
                *,
                lag(point_seq) OVER (PARTITION BY mmsi ORDER BY point_seq) AS previous_point_seq,
                lag(port_group_id) OVER (PARTITION BY mmsi ORDER BY point_seq) AS previous_port_group_id,
                lag(timestamp_utc) OVER (PARTITION BY mmsi ORDER BY point_seq) AS previous_time_utc
            FROM {context}
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


def _write_port_context(
    connection, config: AppConfig, candidates_path: Path, output: Path
) -> int:
    candidates = parquet_sources([candidates_path])
    select_sql = f"""
        SELECT
            mmsi,
            point_seq,
            arg_min(timestamp_utc, candidate_rank) AS timestamp_utc,
            arg_min(latitude, candidate_rank) AS latitude,
            arg_min(longitude, candidate_rank) AS longitude,
            arg_min(speed, candidate_rank) AS speed,
            arg_min(source_at_dock, candidate_rank) AS source_at_dock,
            arg_min(gap_seconds, candidate_rank) AS gap_seconds,
            arg_min(mmsi_bucket, candidate_rank) AS mmsi_bucket,
            arg_min(matched_port_name, candidate_rank) AS matched_port_name,
            arg_min(source_label, candidate_rank) AS source_label,
            arg_min(source_sublabel, candidate_rank) AS source_sublabel,
            arg_min(collection_type, candidate_rank) AS collection_type,
            arg_min(source, candidate_rank) AS source,
            arg_min(port_group_id, candidate_rank) AS port_group_id,
            arg_min(port_group_name, candidate_rank) AS port_group_name,
            arg_min(nearest_anchor_id, candidate_rank) AS nearest_anchor_id,
            max(port_group_id) FILTER (WHERE candidate_rank = 2)
                AS second_port_group_id,
            max(port_group_name) FILTER (WHERE candidate_rank = 2)
                AS second_port_group_name,
            min(port_distance_km) FILTER (WHERE candidate_rank = 1)
                AS port_distance_km,
            min(port_distance_km) FILTER (WHERE candidate_rank = 2)
                AS second_port_distance_km,
            min(port_distance_km) FILTER (WHERE candidate_rank = 2)
                - min(port_distance_km) FILTER (WHERE candidate_rank = 1)
                AS ambiguity_margin_km
        FROM {candidates}
        WHERE candidate_rank <= 2
        GROUP BY mmsi, point_seq
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


def _write_port_call_bucket(
    config: AppConfig,
    track_path: Path,
    stop_matches_path: Path,
    temporary_root: Path,
) -> Dict[str, int]:
    temporary_candidates = temporary_root / "candidates.parquet"
    temporary_context = temporary_root / "port_context.parquet"
    temporary_calls = temporary_root / "port_calls.parquet"
    worker_temp = temporary_root / "duckdb"
    remove_path(worker_temp, temporary_root)
    connection = None
    completed = False
    try:
        connection = open_database(
            config,
            output_reserve_bytes=track_path.stat().st_size * 4,
            workload="bucket",
            worker_temp_directory=worker_temp,
        )
        candidate_count = _write_candidates(
            connection,
            config,
            track_path,
            temporary_candidates,
        )
        context_count = _write_port_context(
            connection,
            config,
            temporary_candidates,
            temporary_context,
        )
        call_count = _write_port_calls(
            connection,
            config,
            temporary_context,
            stop_matches_path,
            temporary_calls,
        )
        completed = True
        return {
            "candidate_count": candidate_count,
            "context_count": context_count,
            "port_call_count": call_count,
        }
    finally:
        if connection is not None:
            connection.close()
        remove_path(worker_temp, temporary_root)
        if not completed:
            remove_path(temporary_root, config.storage.work_root)


def build_port_calls(
    config: AppConfig, project_root: Path, force: bool = False
) -> Dict[str, Any]:
    tracks = track_bucket_files(config)
    ports_root = config.storage.work_root / "stage05_ports"
    dependencies = [
        ports_root / "anchors.parquet",
        ports_root / "anchor_tiles.parquet",
    ]
    if not all(path.exists() for path in dependencies):
        raise RuntimeError("Port artifacts are incomplete; run `extractais ports` first")

    manifest_path = config.storage.work_root / "manifests" / "calls.json"
    manifest = load_stage_manifest(manifest_path, "calls")
    stage_hash = config.stage_hash("ports", "prepare")
    manifest["config_hash"] = stage_hash
    manifest["git_commit"] = git_commit(project_root)
    global_dependency_identity = [
        (str(path.resolve()), path.stat().st_size, path.stat().st_mtime_ns)
        for path in dependencies
    ]
    output_root = config.storage.work_root / "stage06_port_calls"
    tasks: list[BucketTask] = []
    work: Dict[str, Dict[str, Any]] = {}
    completed_samples: list[tuple[int, float]] = []
    completed_count = 0
    for track_path in tracks:
        bucket = bucket_number(track_path)
        stop_matches_path = (
            ports_root
            / "stop_port_matches"
            / f"mmsi_bucket={bucket:04d}"
            / "part.parquet"
        )
        if not stop_matches_path.exists():
            raise RuntimeError(
                f"Stop-to-port matches are incomplete for bucket {bucket:04d}; "
                "run `extractais ports` first"
            )
        source_signature = signature(
            [(str(track_path.resolve()), track_path.stat().st_size, track_path.stat().st_mtime_ns)]
            + global_dependency_identity
            + [
                (
                    str(stop_matches_path.resolve()),
                    stop_matches_path.stat().st_size,
                    stop_matches_path.stat().st_mtime_ns,
                )
            ]
        )
        bucket_root = output_root / f"mmsi_bucket={bucket:04d}"
        candidates_output = bucket_root / "candidates.parquet"
        context_output = bucket_root / "port_context.parquet"
        calls_output = bucket_root / "port_calls.parquet"
        key = f"bucket:{bucket:04d}"
        if not force and item_is_complete(
            manifest,
            key,
            stage_hash,
            source_signature,
            [candidates_output, context_output, calls_output],
        ):
            completed_count += 1
            item = manifest["items"][key]
            completed_samples.append(
                (
                    int(item.get("source_bytes", track_path.stat().st_size)),
                    float(item.get("elapsed_seconds", 0)),
                )
            )
            continue

        temporary_root = temporary_directory(bucket_root)
        remove_path(temporary_root, config.storage.work_root)
        temporary_root.mkdir(parents=True, exist_ok=True)
        source_bytes = track_path.stat().st_size + stop_matches_path.stat().st_size
        work[key] = {
            "source_signature": source_signature,
            "source_bytes": source_bytes,
            "bucket_root": bucket_root,
            "temporary_root": temporary_root,
        }
        tasks.append(
            BucketTask(
                key=key,
                label=f"{bucket:04d}",
                source_bytes=source_bytes,
                estimated_output_bytes=track_path.stat().st_size * 4,
                call=IsolatedCall(
                    key=key,
                    target=_write_port_call_bucket,
                    args=(config, track_path, stop_matches_path, temporary_root),
                ),
            )
        )

    def complete(execution: BucketExecution) -> str:
        item = work[execution.task.key]
        counts = execution.result.value
        temporary_root = item["temporary_root"]
        bucket_root = item["bucket_root"]
        output_bytes = directory_size(temporary_root)
        replace_directory(
            temporary_root, bucket_root, config.storage.work_root
        )
        free_after = free_space_bytes(config.storage.work_root)
        elapsed = execution.result.elapsed_seconds
        source_bytes = item["source_bytes"]
        io_rate = (source_bytes + output_bytes) / 1024**2 / elapsed
        manifest["items"][execution.task.key] = {
            "status": "complete",
            "config_hash": stage_hash,
            "source_signature": item["source_signature"],
            "output": str(bucket_root.resolve()),
            "candidate_count": counts["candidate_count"],
            "context_count": counts["context_count"],
            "port_call_count": counts["port_call_count"],
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
        "recognize port calls",
        tasks,
        total_count=len(tracks),
        completed_count=completed_count,
        completed_samples=completed_samples,
        on_complete=complete,
    )
    return manifest
