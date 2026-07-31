from __future__ import annotations

import csv
from pathlib import Path

import duckdb
import pytest

import extractais.tracks as tracks_module
from extractais.checkpoints import CheckpointStore
from extractais.config import load_config
from extractais.ingest import dynamic_run_path, ingest, static_run_path
from extractais.inventory import discover_files
from extractais.storage import partition_for_mmsi, stop_path, track_path
from extractais.tracks import _reference_canonical_sql, _track_worker, vessel_path
from test_v2_contract import _config, _ports, _raw_track, _row


class _RecordedConnection:
    def __init__(self, connection, closed: list[bool]) -> None:
        self._connection = connection
        self._closed = closed

    def execute(self, *args, **kwargs):
        return self._connection.execute(*args, **kwargs)

    def close(self) -> None:
        self._connection.close()
        self._closed.append(True)


def _ingested_sample(tmp_path: Path):
    raw = tmp_path / "raw"
    year = raw / "2021"
    year.mkdir(parents=True)
    _ports(raw / "ports.csv")
    source = year / "2021-01-01.csv"
    _raw_track(source)
    with source.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            _row(
                timestamp="2021-01-01 01:40:00 UTC",
                MMSI="123456789",
                msg_type="1",
                latitude="0",
                longitude="0.08",
                speed="10",
                at_dock="False",
            )
        )
        writer.writerow(
            _row(
                timestamp="2021-01-01 01:40:00 UTC",
                MMSI="123456789",
                msg_type="1",
                latitude="0",
                longitude="0.081",
                speed="10",
                at_dock="False",
            )
        )

    config = load_config(_config(tmp_path, raw))
    inventory = discover_files(config)
    with CheckpointStore(tmp_path / "ingest-state.sqlite") as store:
        ingest(config, inventory, store)
    return config, inventory


def test_track_worker_materializes_independent_phases_without_changing_rows(
    tmp_path: Path, monkeypatch
) -> None:
    config, inventory = _ingested_sample(tmp_path)
    partition = partition_for_mmsi(123456789, config.layout.track_partitions)
    lane = partition % len(config.storage.track_roots)
    dynamic_paths = [
        dynamic_run_path(config, lane, item.date) for item in inventory.files
    ]
    static_paths = [static_run_path(config, item.date) for item in inventory.files]
    outputs = (
        track_path(config, partition),
        stop_path(config, partition),
        vessel_path(config, partition),
    )
    heartbeat = config.storage.temp_root / "heartbeats" / "tracks-test.json"

    reference = duckdb.connect()
    try:
        expected = reference.execute(
            f"SELECT * FROM ({_reference_canonical_sql(config, dynamic_paths, partition)}) "
            "ORDER BY mmsi, point_seq"
        )
        expected_columns = [item[0] for item in expected.description]
        expected_rows = expected.fetchall()
    finally:
        reference.close()

    real_open_database = tracks_module.open_database
    closed: list[bool] = []
    connection_count = 0

    def recorded_open_database(*args, **kwargs):
        nonlocal connection_count
        connection_count += 1
        return _RecordedConnection(real_open_database(*args, **kwargs), closed)

    monkeypatch.setattr(tracks_module, "open_database", recorded_open_database)
    _track_worker(
        config,
        partition,
        dynamic_paths,
        static_paths,
        outputs,
        heartbeat,
    )

    actual = duckdb.connect()
    try:
        result = actual.execute(
            f"SELECT * FROM read_parquet('{outputs[0].as_posix()}') "
            "ORDER BY mmsi, point_seq"
        )
        actual_columns = [item[0] for item in result.description]
        actual_rows = result.fetchall()
    finally:
        actual.close()

    assert connection_count >= 5
    assert len(closed) == connection_count
    assert actual_columns == expected_columns
    assert actual_rows == expected_rows
    conflict_index = actual_columns.index("is_time_conflict")
    assert sum(bool(row[conflict_index]) for row in actual_rows) == 2
    assert not any(config.storage.temp_root.glob("tracks-*"))


def test_track_worker_restarts_after_an_uncommitted_phase_output(
    tmp_path: Path, monkeypatch
) -> None:
    config, inventory = _ingested_sample(tmp_path)
    partition = partition_for_mmsi(123456789, config.layout.track_partitions)
    lane = partition % len(config.storage.track_roots)
    dynamic_paths = [
        dynamic_run_path(config, lane, item.date) for item in inventory.files
    ]
    static_paths = [static_run_path(config, item.date) for item in inventory.files]
    outputs = (
        track_path(config, partition),
        stop_path(config, partition),
        vessel_path(config, partition),
    )
    heartbeat = config.storage.temp_root / "heartbeats" / "tracks-restart.json"
    real_copy_phase = tracks_module._copy_phase
    failed = False

    def fail_after_sequence(*args, **kwargs):
        nonlocal failed
        result = real_copy_phase(*args, **kwargs)
        if kwargs["phase_index"] == 3 and not failed:
            failed = True
            raise RuntimeError("forced failure after temporary track output")
        return result

    monkeypatch.setattr(tracks_module, "_copy_phase", fail_after_sequence)
    with pytest.raises(RuntimeError, match="forced failure"):
        _track_worker(
            config,
            partition,
            dynamic_paths,
            static_paths,
            outputs,
            heartbeat,
        )

    assert not any(path.exists() for path in outputs)
    assert outputs[0].with_name(outputs[0].stem + ".tmp.parquet").exists()

    monkeypatch.setattr(tracks_module, "_copy_phase", real_copy_phase)
    _track_worker(
        config,
        partition,
        dynamic_paths,
        static_paths,
        outputs,
        heartbeat,
    )

    assert all(path.is_file() for path in outputs)
    assert not outputs[0].with_name(outputs[0].stem + ".tmp.parquet").exists()
    assert not any(config.storage.temp_root.glob("tracks-*"))
