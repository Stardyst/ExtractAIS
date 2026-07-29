import csv
import json
import os
import shutil
import subprocess
from dataclasses import replace
from pathlib import Path

import duckdb
import pytest

from extractais.calls import build_port_calls
from extractais.config import load_config
from extractais.database import open_database
from extractais.intervals import build_intervals
from extractais.inventory import discover_files
from extractais.ports import build_ports
from extractais.prepare import (
    TRACK_LAYOUT_VERSION,
    _ensure_track_layout,
    _track_shard_count,
    _write_track_source_bucket,
    prepare_data,
)
from extractais.split import split_files
from extractais.stops import build_stops
from extractais.validate import build_validation
from test_split import COLUMNS, _row


def _pipeline_config(tmp_path: Path, raw_root: Path, work_root: Path) -> Path:
    config = tmp_path / "pipeline.yaml"
    config.write_text(
        f"""
input:
  raw_root: "{raw_root.as_posix()}"
  year_directories:
    2021: "2021"
  filename_pattern: "*.csv"
  ports_csv: "{(raw_root / 'ports.csv').as_posix()}"
storage:
  work_root: "{work_root.as_posix()}"
  temp_directory: "{(work_root / 'tmp').as_posix()}"
runtime:
  threads: 2
  memory_limit: "1GB"
  bucket_workers: 2
  bucket_threads: 1
  bucket_memory_limit: "384MB"
  bucket_temp_limit_gb: 1
  enable_progress: false
  minimum_free_space_gb: 0
split:
  dynamic_message_types: [1, 2, 3, 18, 19, 27]
  static_message_types: [5, 24]
  compression: "zstd"
  row_group_size: 10000
prepare:
  mmsi_buckets: 2
  partition_write_max_open_files: 2
  compression: "zstd"
  row_group_size: 10000
  max_implied_speed_knots: 1000
stops:
  max_speed_knots: 0.5
  max_step_km: 0.25
  max_point_gap_minutes: 60
  min_duration_minutes: 60
  max_diameter_km: 1.0
ports:
  entry_radius_km: 3
  exit_radius_km: 4
  approach_radius_km: 10
  group_distance_km: 4
  anchor_assignment_radius_km: 20
  anchor_grid_degrees: 0.005
  anchor_min_stop_events: 1
  anchor_min_vessels: 1
  call_min_points: 2
  call_max_point_gap_hours: 6
  ambiguity_margin_km: 1
  excluded_harbor_sizes: []
intervals:
  unknown_gap_hours: 6
""".strip(),
        encoding="utf-8",
    )
    return config


def _write_ports(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "World Port Index Number", "Main Port Name", "Alternate Port Name",
                "UN/LOCODE", "Country Code", "Region Name", "Harbor Size",
                "Harbor Type", "Latitude", "Longitude",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "World Port Index Number": "1", "Main Port Name": "Port A",
                "Harbor Size": "Large", "Latitude": "0", "Longitude": "0",
            }
        )
        writer.writerow(
            {
                "World Port Index Number": "2", "Main Port Name": "Port B",
                "Harbor Size": "Large", "Latitude": "0", "Longitude": "1",
            }
        )


def _write_track(path: Path) -> None:
    points = [
        ("2021-01-01 00:00:00 UTC", 0.0000, 0.0000, 0.0, True),
        ("2021-01-01 00:30:00 UTC", 0.0002, 0.0001, 0.0, True),
        ("2021-01-01 01:00:00 UTC", 0.0001, 0.0002, 0.0, True),
        ("2021-01-01 01:20:00 UTC", 0.0000, 0.0200, 5.0, False),
        ("2021-01-01 01:30:00 UTC", 0.0000, 0.0600, 10.0, False),
        ("2021-01-01 02:00:00 UTC", 0.0000, 0.2000, 12.0, False),
        ("2021-01-01 09:00:00 UTC", 0.0000, 0.9200, 8.0, False),
        ("2021-01-01 09:10:00 UTC", 0.0000, 0.9800, 3.0, False),
        ("2021-01-01 09:20:00 UTC", 0.0000, 1.0000, 0.0, True),
        ("2021-01-01 10:20:00 UTC", 0.0001, 1.0001, 0.0, True),
        ("2021-01-01 11:20:00 UTC", 0.0000, 1.0002, 0.0, True),
    ]
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        for timestamp, latitude, longitude, speed, at_dock in points:
            writer.writerow(
                _row(
                    timestamp=timestamp,
                    MMSI="123456789",
                    msg_type="1",
                    latitude=str(latitude),
                    longitude=str(longitude),
                    speed=str(speed),
                    at_dock=str(at_dock),
                )
            )
        writer.writerow(
            _row(
                timestamp="2021-01-01 00:05:00 UTC",
                MMSI="123456789",
                msg_type="5",
                IMO="9876543",
                ship_type="70",
                length="200",
                width="30",
            )
        )


def test_complete_pipeline_builds_port_calls_states_and_validation(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    _write_ports(raw_root / "ports.csv")
    _write_track(year / "2021-01-01.csv")
    work_root = tmp_path / "work"
    config = load_config(_pipeline_config(tmp_path, raw_root, work_root))

    inventory = discover_files(config)
    split_files(config, inventory.files, tmp_path)
    prepare_data(config, tmp_path)
    month_files = list(
        (work_root / "stage02_partitioned" / "year=2021" / "month=01").rglob(
            "*.parquet"
        )
    )
    assert len(month_files) <= config.prepare.mmsi_buckets
    build_stops(config, tmp_path)
    build_ports(config, tmp_path)
    build_port_calls(config, tmp_path)
    build_intervals(config, tmp_path)
    build_validation(config, tmp_path)

    calls = duckdb.read_parquet(
        str(work_root / "stage06_port_calls" / "mmsi_bucket=*" / "port_calls.parquet")
    ).fetchall()
    assert len(calls) == 2
    assert list(
        (work_root / "stage05_ports" / "stop_port_matches").glob(
            "mmsi_bucket=*/part.parquet"
        )
    )
    assert list(
        (work_root / "stage06_port_calls").glob(
            "mmsi_bucket=*/port_context.parquet"
        )
    )
    assert not (work_root / "stage07_intervals").exists()

    interval_glob = str(
        work_root / "outputs" / "trajectory_intervals" / "year=2021" / "*.parquet"
    )
    columns = {
        row[0]
        for row in duckdb.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{interval_glob}')"
        ).fetchall()
    }
    assert not {
        "start_latitude", "start_longitude", "end_latitude", "end_longitude",
        "duration_seconds", "distance_km",
    } & columns
    states = {
        row[0]
        for row in duckdb.sql(
            f"SELECT DISTINCT state FROM read_parquet('{interval_glob}')"
        ).fetchall()
    }
    assert {"IN_PORT", "DEPARTING", "OCEAN", "ARRIVING", "UNKNOWN_GAP"} <= states
    assert (work_root / "outputs" / "validation" / "port_quality.csv").exists()
    assert (work_root / "outputs" / "validation" / "summary.json").exists()

    for stage in ("prepare", "stops", "ports", "calls", "intervals", "validate"):
        stage_manifest = json.loads(
            (work_root / "manifests" / f"{stage}.json").read_text(encoding="utf-8")
        )
        worker_process_ids = {
            item["worker_process_id"]
            for item in stage_manifest["items"].values()
        }
        assert all(
            item["free_space_bytes_before"] > 0
            and item["free_space_bytes_after"] > 0
            for item in stage_manifest["items"].values()
        )
        assert worker_process_ids
        assert os.getpid() not in worker_process_ids

    interval_file = next(
        (work_root / "outputs" / "trajectory_intervals").glob(
            "year=*/mmsi_bucket=*.parquet"
        )
    )
    modified_time = interval_file.stat().st_mtime_ns
    build_intervals(config, tmp_path)
    assert interval_file.stat().st_mtime_ns == modified_time
    interval_manifest = json.loads(
        (work_root / "manifests" / "intervals.json").read_text(
            encoding="utf-8"
        )
    )
    assert "yearly_export" not in interval_manifest["items"]


def test_stop_stage_can_commit_an_empty_bucket(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    _write_ports(raw_root / "ports.csv")
    csv_path = year / "2021-01-01.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        for hour, longitude in ((0, 0.0), (1, 0.2), (2, 0.4)):
            writer.writerow(
                _row(
                    timestamp=f"2021-01-01 {hour:02d}:00:00 UTC",
                    MMSI="123456789",
                    msg_type="1",
                    latitude="0",
                    longitude=str(longitude),
                    speed="12",
                )
            )

    work_root = tmp_path / "work"
    config = load_config(_pipeline_config(tmp_path, raw_root, work_root))
    inventory = discover_files(config)
    split_files(config, inventory.files, tmp_path)
    prepare_data(config, tmp_path)

    build_stops(config, tmp_path)

    stop_file = next((work_root / "stage04_stops").glob("mmsi_bucket=*/part.parquet"))
    assert duckdb.read_parquet(str(stop_file)).fetchall() == []


def test_prepare_stops_before_work_when_storage_budget_is_insufficient(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    _write_ports(raw_root / "ports.csv")
    _write_track(year / "2021-01-01.csv")
    work_root = tmp_path / "work"
    config_path = _pipeline_config(tmp_path, raw_root, work_root)
    config = load_config(config_path)
    inventory = discover_files(config)
    split_files(config, inventory.files, tmp_path)

    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "minimum_free_space_gb: 0",
            "minimum_free_space_gb: 1000000",
        ),
        encoding="utf-8",
    )
    guarded_config = load_config(config_path)

    with pytest.raises(RuntimeError, match="Storage guard stopped"):
        prepare_data(guarded_config, tmp_path)

    assert not (work_root / "stage02_partitioned").exists()


def test_missing_space_reserve_defaults_to_500_gib(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    config_path = _pipeline_config(tmp_path, raw_root, tmp_path / "work")
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "  minimum_free_space_gb: 0\n", ""
        ),
        encoding="utf-8",
    )
    config = load_config(config_path)

    assert config.runtime.minimum_free_space_gb == 500


def test_legacy_runtime_config_gets_resource_defaults(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    config_path = _pipeline_config(tmp_path, raw_root, tmp_path / "work")
    config_path.write_text(
        config_path.read_text(encoding="utf-8")
        .replace("  threads: 2\n", "")
        .replace('  memory_limit: "1GB"\n', "")
        .replace("  bucket_workers: 2\n", "")
        .replace("  bucket_threads: 1\n", "")
        .replace('  bucket_memory_limit: "384MB"\n', ""),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.runtime.threads == 20
    assert config.runtime.memory_limit == "90GB"
    assert config.runtime.bucket_workers == 1
    assert config.runtime.bucket_threads == 8
    assert config.runtime.bucket_memory_limit == "80GB"


def test_production_example_uses_bounded_partition_writers() -> None:
    project_root = Path(__file__).parents[1]
    config = load_config(project_root / "configs" / "production.example.yaml")

    assert config.prepare.mmsi_buckets == 256
    assert config.prepare.partition_write_max_open_files == 256
    assert config.prepare.row_group_size == 250_000
    assert config.runtime.minimum_free_space_gb == 500
    assert config.runtime.bucket_workers == 1
    assert config.runtime.bucket_threads == 8
    assert config.runtime.bucket_memory_limit == "80GB"
    assert config.runtime.bucket_temp_limit_gb == 256
    assert _track_shard_count(config, int(13.33 * 1024**3)) == 32
    assert _track_shard_count(config, int(3.27 * 1024**3)) == 8


def test_partition_writer_rejects_fewer_open_files_than_buckets(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    config_path = _pipeline_config(tmp_path, raw_root, tmp_path / "work")
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "partition_write_max_open_files: 2",
            "partition_write_max_open_files: 1",
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="at least prepare.mmsi_buckets"):
        load_config(config_path)


def test_duckdb_keeps_one_partition_writer_per_bucket(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    config = load_config(_pipeline_config(tmp_path, raw_root, tmp_path / "work"))

    connection = open_database(config)
    try:
        maximum_open_files = int(
            connection.execute(
                "SELECT current_setting('partitioned_write_max_open_files')"
            ).fetchone()[0]
        )
    finally:
        connection.close()

    assert maximum_open_files == config.prepare.partition_write_max_open_files
    assert maximum_open_files >= config.prepare.mmsi_buckets


def test_bucket_database_uses_bounded_resources_and_no_internal_eta(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    config = load_config(_pipeline_config(tmp_path, raw_root, tmp_path / "work"))

    connection = open_database(config, workload="bucket")
    try:
        threads = int(
            connection.execute("SELECT current_setting('threads')").fetchone()[0]
        )
        progress_enabled = bool(
            connection.execute(
                "SELECT current_setting('enable_progress_bar')"
            ).fetchone()[0]
        )
    finally:
        connection.close()

    assert threads == config.runtime.bucket_threads
    assert progress_enabled is False


def test_track_bucket_completes_with_spilling_under_constrained_memory(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    config = load_config(_pipeline_config(tmp_path, raw_root, tmp_path / "work"))
    config = replace(
        config,
        runtime=replace(
            config.runtime,
            bucket_threads=8,
            bucket_memory_limit="64MB",
            bucket_temp_limit_gb=1,
        ),
    )
    source = tmp_path / "large-bucket.parquet"
    tracks_root = tmp_path / "tracks"
    duckdb.execute(
        """
        COPY (
            SELECT
                123000000 + (i % 1000)::BIGINT AS mmsi,
                TIMESTAMP '2021-01-01'
                    + (i % 200000) * INTERVAL 1 SECOND AS timestamp_utc,
                ((i % 10000)::DOUBLE) / 100000 AS latitude,
                ((i % 15000)::DOUBLE) / 100000 AS longitude,
                (i % 20)::DOUBLE AS speed,
                (i % 2) = 0 AS source_at_dock,
                1::INTEGER AS msg_type,
                (i % 100000)::BIGINT AS msg_id,
                0::INTEGER AS mmsi_bucket
            FROM range(1000000) rows(i)
        ) TO ? (FORMAT PARQUET, ROW_GROUP_SIZE 100000)
        """,
        [str(source)],
    )
    expected_rows = duckdb.execute(
        """
        SELECT count(*)
        FROM (
            SELECT DISTINCT mmsi, timestamp_utc, latitude, longitude
            FROM read_parquet(?)
        )
        """,
        [str(source)],
    ).fetchone()[0]
    expected_conflict_rows = duckdb.execute(
        """
        WITH deduplicated AS (
            SELECT DISTINCT mmsi, timestamp_utc, latitude, longitude
            FROM read_parquet(?)
        ),
        times AS (
            SELECT mmsi, timestamp_utc, count(*) AS point_count
            FROM deduplicated
            GROUP BY mmsi, timestamp_utc
        )
        SELECT coalesce(sum(point_count) FILTER (WHERE point_count > 1), 0)
        FROM times
        """,
        [str(source)],
    ).fetchone()[0]

    result = _write_track_source_bucket(
        config,
        [source],
        0,
        tracks_root,
        "test-source-signature",
        "test-track-hash",
        True,
    )

    outputs = [Path(path) for path in result["outputs"]]
    assert result["row_count"] == expected_rows
    assert len(outputs) > 1
    tracks = duckdb.read_parquet([str(path) for path in outputs])
    assert tracks.aggregate("count(*), min(point_seq), max(point_seq)").fetchone() == (
        expected_rows,
        1,
        expected_rows // 1000,
    )
    assert tracks.aggregate("count(*) FILTER (WHERE is_time_conflict)").fetchone()[
        0
    ] == expected_conflict_rows
    split_vessels = duckdb.execute(
        """
        SELECT count(*)
        FROM (
            SELECT mmsi
            FROM read_parquet(?, filename = true)
            GROUP BY mmsi
            HAVING count(DISTINCT filename) > 1
        )
        """,
        [[str(path) for path in outputs]],
    ).fetchone()[0]
    assert split_vessels == 0

    preserved = outputs[0]
    missing = outputs[-1]
    preserved_mtime = preserved.stat().st_mtime_ns
    missing.unlink()
    resumed = _write_track_source_bucket(
        config,
        [source],
        0,
        tracks_root,
        "test-source-signature",
        "test-track-hash",
        False,
    )
    assert len(resumed["outputs"]) == len(outputs)
    assert missing.exists()
    assert preserved.stat().st_mtime_ns == preserved_mtime
    assert not list(config.storage.temp_directory.rglob("*"))


def test_track_layout_migration_preserves_upstream_and_clears_dependents(
    tmp_path: Path,
) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    _write_ports(raw_root / "ports.csv")
    work_root = tmp_path / "work"
    config = load_config(_pipeline_config(tmp_path, raw_root, work_root))
    partition = work_root / "stage02_partitioned" / "sentinel.parquet"
    static = work_root / "stage02_static" / "vessels.parquet"
    legacy = work_root / "stage03_tracks" / "mmsi_bucket=0001" / "part.parquet"
    stale_downstream = [
        work_root / "stage04_stops" / "mmsi_bucket=0032" / "part.parquet",
        work_root / "stage05_ports" / "anchors.parquet",
        work_root / "stage06_port_calls" / "mmsi_bucket=0032" / "port_calls.parquet",
        work_root / "outputs" / "trajectory_intervals" / "year=2021" / "mmsi_bucket=0032.parquet",
        work_root / "outputs" / "validation" / "summary.json",
    ]
    for path in (partition, static, legacy):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"preserve" if path != legacy else b"legacy")
    for path in stale_downstream:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(b"stale")
    manifest_path = work_root / "manifests" / "prepare.json"
    for stage in ("stops", "ports", "calls", "intervals", "validate"):
        downstream_manifest = work_root / "manifests" / f"{stage}.json"
        downstream_manifest.parent.mkdir(parents=True, exist_ok=True)
        downstream_manifest.write_text("{}", encoding="utf-8")
    manifest = {
        "stage": "prepare",
        "items": {
            "partition:2021-01": {"status": "complete"},
            "static": {"status": "complete"},
            "track:0001": {"status": "complete"},
        },
    }

    _ensure_track_layout(
        config,
        work_root / "stage03_tracks",
        manifest,
        manifest_path,
    )

    marker = json.loads(
        (work_root / "stage03_tracks" / "_layout.json").read_text(encoding="utf-8")
    )
    assert marker["version"] == TRACK_LAYOUT_VERSION
    assert partition.read_bytes() == b"preserve"
    assert static.read_bytes() == b"preserve"
    assert not legacy.exists()
    assert not any(path.exists() for path in stale_downstream)
    assert not any(
        (work_root / "manifests" / f"{stage}.json").exists()
        for stage in ("stops", "ports", "calls", "intervals", "validate")
    )
    assert set(manifest["items"]) == {"partition:2021-01", "static"}


def test_diagnostic_classifies_file_multiplication_in_prepare() -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is required for the diagnostic script test")

    helper = Path(__file__).parents[1] / "scripts" / "prepare_diagnostic_findings.ps1"
    command = f"""
. '{helper.as_posix()}'
$summary = [pscustomobject]@{{
    AverageCpuPercent = 3.89
    AverageCpuCores = 1.25
    PeakPrivateGiB = 5.89
    MinimumAvailableMemoryGiB = 99.25
    PeakHandles = 603
    AverageProcessReadMiBps = 0
    AverageProcessWriteMiBps = 6.21
    AverageDiskReadMiBps = 0
    AverageDiskWriteMiBps = 6.16
    AverageDiskBusyPercent = 7.29
    AverageDiskQueue = 0
    PeakDiskQueue = 0
    OutputGrowthGiB = 0.82
    OutputFilesAtEnd = 17754
    TempGrowthGiB = 0
    FreeSpaceChangeGiB = -0.85
}}
$findings = @(Get-PrepareDiagnosticFindings -Summary $summary -Phase 'prepare' -MmsiBuckets 256)
ConvertTo-Json -InputObject $findings -Compress
"""
    result = subprocess.run(
        [powershell, "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
    )

    findings = json.loads(result.stdout)
    assert any("partition file multiplication" in finding.lower() for finding in findings)
