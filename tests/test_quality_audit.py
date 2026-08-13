from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from extractais import cli
from extractais.audit import _behavior_audit_worker, _track_audit_worker
from extractais.audit import build_quality_audit
from extractais.checkpoints import CheckpointStore
from extractais.config import load_config
from extractais.intervals import interval_path
from extractais.pipeline import checkpoint_path, run_pipeline
from extractais.storage import port_call_path, track_path
from extractais.validate import validation_partial_paths
from test_v2_contract import _config, _ports, _raw_track


def _copy_query(path: Path, query: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect()
    try:
        connection.execute(
            "COPY (" + query + ") TO ? (FORMAT PARQUET)", [str(path.resolve())]
        )
    finally:
        connection.close()


def _audit_config(tmp_path: Path):
    raw = tmp_path / "raw"
    raw.mkdir()
    _ports(raw / "ports.csv")
    return load_config(_config(tmp_path, raw))


def test_track_audit_separates_flagged_points_from_spatial_conflict(
    tmp_path: Path,
) -> None:
    config = _audit_config(tmp_path)
    partition = 0
    _copy_query(
        track_path(config, partition),
        """
        SELECT * FROM (VALUES
            (123456789::BIGINT, 1::BIGINT, TIMESTAMP '2021-01-01 00:00:00',
             0.0::DOUBLE, 0.0::DOUBLE, true),
            (123456789::BIGINT, 2::BIGINT, TIMESTAMP '2021-01-01 00:00:00',
             0.0::DOUBLE, 0.0005::DOUBLE, true),
            (123456789::BIGINT, 3::BIGINT, TIMESTAMP '2021-01-01 01:00:00',
             0.0::DOUBLE, 1.0::DOUBLE, false)
        ) AS t(mmsi, point_seq, timestamp_utc, latitude, longitude,
               is_time_conflict)
        """,
    )
    outputs = (
        tmp_path / "track-summary.parquet",
        tmp_path / "conflict-bins.parquet",
        tmp_path / "conflict-examples.parquet",
    )

    _track_audit_worker(
        config,
        partition,
        outputs,
        tmp_path / "track-heartbeat.json",
    )

    summary = duckdb.read_parquet(str(outputs[0])).fetchone()
    columns = duckdb.read_parquet(str(outputs[0])).columns
    values = dict(zip(columns, summary))
    assert values["point_count"] == 3
    assert values["flagged_time_conflict_point_count"] == 2
    assert values["time_conflict_timestamp_count"] == 1

    conflict_bin = duckdb.read_parquet(str(outputs[1])).fetchone()
    assert conflict_bin[0] == "LE_100_M"
    assert conflict_bin[1] == 1
    assert conflict_bin[2] == 2
    assert conflict_bin[3] == pytest.approx(0.0556, rel=0.03)


def test_behavior_audit_measures_call_ambiguity_and_gap_duration(
    tmp_path: Path,
) -> None:
    config = _audit_config(tmp_path)
    partition = 0
    _copy_query(
        interval_path(config, 2021, partition),
        """
        SELECT * FROM (VALUES
            (2021, 123456789::BIGINT, 's1', TIMESTAMP '2021-01-01 00:00:00',
             TIMESTAMP '2021-01-01 00:30:00', 'ARRIVING', NULL, 'C1', 4::BIGINT),
            (2021, 123456789::BIGINT, 's2', TIMESTAMP '2021-01-01 00:30:00',
             TIMESTAMP '2021-01-01 01:30:00', 'IN_PORT', 'C1', 'C1', 10::BIGINT),
            (2021, 123456789::BIGINT, 's3', TIMESTAMP '2021-01-01 01:30:00',
             TIMESTAMP '2021-01-01 02:00:00', 'DEPARTING', 'C1', NULL, 4::BIGINT),
            (2021, 123456789::BIGINT, 's4', TIMESTAMP '2021-01-01 02:00:00',
             TIMESTAMP '2021-01-01 03:00:00', 'OCEAN', 'C1', NULL, 6::BIGINT),
            (2021, 123456789::BIGINT, 's5', TIMESTAMP '2021-01-01 03:00:00',
             TIMESTAMP '2021-01-01 11:00:00', 'UNKNOWN_GAP', 'C1', NULL, 0::BIGINT)
        ) AS raw(year, mmsi, segment_id, start_time_utc, end_time_utc, state,
                 from_port_call_id, to_port_call_id, ais_point_count)
        CROSS JOIN LATERAL (
            SELECT false AS has_time_conflict, false AS has_port_ambiguity
        ) flags
        """,
    )
    _copy_query(
        port_call_path(config, partition),
        """
        SELECT * FROM (VALUES
            ('C1', 123456789::BIGINT, 0::INTEGER, 1::BIGINT, 'PG-1', 'Port A',
             'Area Alpha', TIMESTAMP '2021-01-01 00:00:00',
             TIMESTAMP '2021-01-01 01:30:00',
             TIMESTAMP '2021-01-01 00:30:00',
             TIMESTAMP '2021-01-01 01:30:00', 10::BIGINT, 0.1::DOUBLE,
             1::BIGINT, 0::BIGINT, 1::BIGINT, 0.1::DOUBLE, true)
        ) AS t(port_call_id, mmsi, track_partition_id, episode_number,
               port_group_id, port_group_name, port_country_or_area,
               approach_start_time_utc, approach_end_time_utc, entry_time_utc,
               exit_time_utc, point_count, minimum_port_distance_km,
               source_at_dock_points, source_matched_port_points,
               matched_stop_count, minimum_ambiguity_margin_km,
               has_port_ambiguity)
        """,
    )
    ambiguous = validation_partial_paths(config, partition)[2]
    _copy_query(
        ambiguous,
        """
        SELECT * FROM (VALUES
            (123456789::BIGINT, 5::BIGINT, TIMESTAMP '2021-01-01 01:00:00',
             0.0::DOUBLE, 0.001::DOUBLE, 'PG-1', 'Port A', 'PG-2', 'Port B',
             0.1::DOUBLE, 0.2::DOUBLE, 0.1::DOUBLE, 0::INTEGER)
        ) AS t(mmsi, point_seq, timestamp_utc, latitude, longitude,
               port_group_id, port_group_name, second_port_group_id,
               second_port_group_name, port_distance_km,
               second_port_distance_km, ambiguity_margin_km,
               track_partition_id)
        """,
    )
    outputs = (
        tmp_path / "gap-summary.parquet",
        tmp_path / "vessel-gap-coverage.parquet",
        tmp_path / "transitions.parquet",
        tmp_path / "call-evidence.parquet",
        tmp_path / "call-review.parquet",
        tmp_path / "call-review-points.parquet",
        tmp_path / "port-pairs.parquet",
        tmp_path / "interval-flags.parquet",
        tmp_path / "ambiguity-assignment.parquet",
    )

    _behavior_audit_worker(
        config,
        partition,
        outputs,
        tmp_path / "behavior-heartbeat.json",
    )

    gap = duckdb.read_parquet(str(outputs[0])).fetchone()
    gap_values = dict(zip(duckdb.read_parquet(str(outputs[0])).columns, gap))
    assert gap_values["duration_bin"] == "06_TO_12_HOURS"
    assert gap_values["gap_count"] == 1
    assert gap_values["total_gap_seconds"] == 8 * 3600

    coverage = duckdb.read_parquet(str(outputs[1])).fetchone()
    coverage_values = dict(
        zip(duckdb.read_parquet(str(outputs[1])).columns, coverage)
    )
    assert coverage_values["coverage_bin"] == "GT_50_PERCENT"
    assert coverage_values["vessel_count"] == 1
    assert coverage_values["total_active_span_seconds"] == 11 * 3600
    assert coverage_values["total_gap_seconds"] == 8 * 3600

    review = duckdb.read_parquet(str(outputs[4]))
    call = review.fetchone()
    call_values = dict(zip(review.columns, call))
    assert call_values["ambiguous_point_count"] == 1
    assert call_values["ambiguous_point_fraction"] == pytest.approx(0.1)
    assert call_values["has_port_ambiguity"] is True
    assert call_values["has_arriving_interval"] is True
    assert call_values["has_in_port_interval"] is True
    assert call_values["has_departing_interval"] is True
    assert call_values["closest_competing_port_group_id"] == "PG-2"
    review_point = duckdb.read_parquet(str(outputs[5])).fetchone()
    assert review_point[1] == "C1"

    transitions = {
        (row[1], row[2]): row[3]
        for row in duckdb.read_parquet(str(outputs[2])).fetchall()
    }
    assert transitions[("ARRIVING", "IN_PORT")] == 1
    assert transitions[("IN_PORT", "DEPARTING")] == 1
    assert transitions[("DEPARTING", "OCEAN")] == 1
    assert transitions[("OCEAN", "UNKNOWN_GAP")] == 1


def test_quality_audit_is_resumable_and_does_not_modify_pipeline_products(
    tmp_path: Path,
) -> None:
    raw = tmp_path / "raw"
    year = raw / "2021"
    year.mkdir(parents=True)
    _ports(raw / "ports.csv")
    _raw_track(year / "2021-01-01.csv")
    config = load_config(_config(tmp_path, raw))
    run_pipeline(config, tmp_path)

    source_paths = [
        track_path(config, partition)
        for partition in range(config.layout.track_partitions)
    ] + [
        interval_path(config, 2021, partition)
        for partition in range(config.layout.track_partitions)
    ] + [
        port_call_path(config, partition)
        for partition in range(config.layout.track_partitions)
    ]
    source_mtimes = {path: path.stat().st_mtime_ns for path in source_paths}

    with CheckpointStore(checkpoint_path(config)) as store:
        build_quality_audit(config, store)

    audit_root = config.storage.products_root / "quality_audit"
    expected = {
        "summary.json",
        "time_conflict_summary.csv",
        "unknown_gap_duration.csv",
        "port_call_quality.csv",
        "competing_port_pairs.csv",
        "review_calls.parquet",
        "review_ambiguous_points.parquet",
    }
    assert expected <= {path.name for path in audit_root.iterdir() if path.is_file()}
    audit_mtimes = {
        path: path.stat().st_mtime_ns
        for path in audit_root.iterdir()
        if path.is_file()
    }

    with CheckpointStore(checkpoint_path(config)) as store:
        build_quality_audit(config, store)

    assert {path: path.stat().st_mtime_ns for path in source_paths} == source_mtimes
    assert {path: path.stat().st_mtime_ns for path in audit_mtimes} == audit_mtimes


def test_audit_cli_does_not_inventory_raw_csv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = _audit_config(tmp_path)
    called = []

    monkeypatch.setattr(
        cli,
        "_inventory",
        lambda _config: pytest.fail("audit-quality must not inventory raw CSV"),
    )
    monkeypatch.setattr(
        cli,
        "build_quality_audit",
        lambda received, _store: called.append(received),
    )
    monkeypatch.setattr(cli, "_status", lambda _config: {"ok": True})
    monkeypatch.setattr(cli, "_print_status", lambda _status, _json: None)

    cli.main(["--config", str(config.source_path), "audit-quality"])

    assert called == [config]
