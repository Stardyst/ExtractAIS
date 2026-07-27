import csv
import json
import os
import shutil
import subprocess
from pathlib import Path

import duckdb
import pytest

from extractais.calls import build_port_calls
from extractais.config import load_config
from extractais.database import open_database
from extractais.intervals import build_intervals
from extractais.inventory import discover_files
from extractais.ports import build_ports
from extractais.prepare import prepare_data
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
  partition_write_max_open_files: 1
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
        .replace('  memory_limit: "1GB"\n', ""),
        encoding="utf-8",
    )

    config = load_config(config_path)

    assert config.runtime.threads == 20
    assert config.runtime.memory_limit == "90GB"


def test_production_example_uses_bounded_partition_writers() -> None:
    project_root = Path(__file__).parents[1]
    config = load_config(project_root / "configs" / "production.example.yaml")

    assert config.prepare.mmsi_buckets == 256
    assert config.prepare.partition_write_max_open_files == 100
    assert config.prepare.row_group_size == 250_000
    assert config.runtime.minimum_free_space_gb == 500
    assert config.runtime.bucket_workers == 2
    assert config.runtime.bucket_threads == 10
    assert config.runtime.bucket_memory_limit == "42GB"
    assert config.runtime.bucket_temp_limit_gb == 256


def test_duckdb_partition_writer_limits_open_files_independently_of_buckets(
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
    assert maximum_open_files < config.prepare.mmsi_buckets


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


def test_diagnostic_classifies_partition_writer_fanout() -> None:
    powershell = shutil.which("powershell") or shutil.which("pwsh")
    if powershell is None:
        pytest.skip("PowerShell is required for the diagnostic script test")

    helper = Path(__file__).parents[1] / "scripts" / "prepare_diagnostic_findings.ps1"
    command = f"""
. '{helper.as_posix()}'
$summary = [pscustomobject]@{{
    AverageCpuPercent = 3.77
    AverageCpuCores = 1.21
    PeakPrivateGiB = 63.56
    MinimumAvailableMemoryGiB = 40.30
    PeakHandles = 1036
    AverageProcessReadMiBps = 48.71
    AverageProcessWriteMiBps = 64.59
    AverageDiskReadMiBps = 0
    AverageDiskWriteMiBps = 41.09
    AverageDiskBusyPercent = 64.78
    AverageDiskQueue = 0.53
    PeakDiskQueue = 18
    OutputGrowthGiB = 0
    OutputFilesAtEnd = 512
    TempGrowthGiB = 0.07
    FreeSpaceChangeGiB = -8.29
}}
$findings = @(Get-PrepareDiagnosticFindings -Summary $summary -Phase 'partition_month' -MmsiBuckets 512)
ConvertTo-Json -InputObject $findings -Compress
"""
    result = subprocess.run(
        [powershell, "-NoProfile", "-Command", command],
        check=True,
        capture_output=True,
        text=True,
    )

    findings = json.loads(result.stdout)
    assert any("partition writer fan-out" in finding.lower() for finding in findings)
