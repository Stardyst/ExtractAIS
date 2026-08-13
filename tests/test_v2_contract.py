from __future__ import annotations

import csv
from pathlib import Path

import duckdb

from extractais.checkpoints import CheckpointStore
from extractais.config import load_config
from extractais.pipeline import run_pipeline
from extractais.progress import StageProgress
from extractais.storage import lane_for_partition, partition_for_mmsi

COLUMNS = [
    "timestamp", "MMSI", "msg_type", "latitude", "longitude", "speed",
    "course", "heading", "rot", "IMO", "flag", "draught",
    "ship_and_cargo_type", "length", "width", "eta", "status", "maneuver",
    "accuracy", "to_bow", "to_stern", "to_port", "to_starboard",
    "collection_type", "matchedPortName", "PipelineExecutionId", "source",
    "msg_id", "ais_version", "ship_type", "geopoint_index_id", "DateOnly",
    "s2id", "label", "sublabel", "iso3", "at_dock",
]


def _row(**values):
    return [values.get(column, "") for column in COLUMNS]


def _config(tmp_path: Path, raw_root: Path) -> Path:
    config = tmp_path / "v2.yaml"
    config.write_text(
        f"""
input:
  raw_root: "{raw_root.as_posix()}"
  year_directories:
    2021: "2021"
  filename_pattern: "*.csv"
  ports_csv: "{(raw_root / 'ports.csv').as_posix()}"
storage:
  track_roots:
    - "{(tmp_path / 'track-a').as_posix()}"
    - "{(tmp_path / 'track-b').as_posix()}"
  temp_root: "{(tmp_path / 'temp').as_posix()}"
  products_root: "{(tmp_path / 'products').as_posix()}"
  evidence_roots:
    - "{(tmp_path / 'evidence-a').as_posix()}"
    - "{(tmp_path / 'evidence-b').as_posix()}"
  reserves_gib:
    tracks: 0
    temp: 0
    products: 0
    evidence: 0
runtime:
  global_threads: 2
  global_memory: "1GB"
  workers_per_track_root: 1
  worker_threads: 1
  worker_memory: "512MB"
  worker_temp_gib: 1
  progress_interval_seconds: 0.05
  enable_progress: false
layout:
  track_partitions: 4
  row_group_size: 1000
  compression: "zstd"
cleaning:
  dynamic_message_types: [1, 2, 3, 18, 19, 27]
  static_message_types: [5, 24]
  max_implied_speed_knots: 1000
stops:
  max_speed_knots: 0.5
  max_step_km: 0.25
  max_point_gap_minutes: 60
  min_duration_minutes: 20
  max_diameter_km: 1
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


def _ports(path: Path) -> None:
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "World Port Index Number",
                "Main Port Name",
                "Alternate Port Name",
                "UN/LOCODE",
                "Country Code",
                "Region Name",
                "Harbor Size",
                "Harbor Type",
                "Latitude",
                "Longitude",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "World Port Index Number": "1",
                "Main Port Name": "Port A",
                "Country Code": "Area Alpha",
                "Harbor Size": "Large",
                "Latitude": "0",
                "Longitude": "0",
            }
        )
        writer.writerow(
            {
                "World Port Index Number": "2",
                "Main Port Name": "Port A Terminal",
                "Country Code": "Area Border",
                "Harbor Size": "Small",
                "Latitude": "0",
                "Longitude": "0.01",
            }
        )
        writer.writerow(
            {
                "World Port Index Number": "3",
                "Main Port Name": "Port B",
                "Country Code": "Area Beta",
                "Harbor Size": "Large",
                "Latitude": "0",
                "Longitude": "1",
            }
        )


def _raw_track(path: Path) -> None:
    points = [
        ("2021-01-01 00:00:00 UTC", 0.0000, 0.0000, 0.0, True),
        ("2021-01-01 00:30:00 UTC", 0.0001, 0.0001, 0.0, True),
        ("2021-01-01 01:00:00 UTC", 0.0002, 0.0001, 0.0, True),
        ("2021-01-01 01:20:00 UTC", 0.0000, 0.0200, 5.0, False),
        ("2021-01-01 01:40:00 UTC", 0.0000, 0.0800, 10.0, False),
        ("2021-01-01 02:00:00 UTC", 0.0000, 0.2000, 12.0, False),
        ("2021-01-01 09:00:00 UTC", 0.0000, 0.9200, 8.0, False),
        ("2021-01-01 09:10:00 UTC", 0.0000, 0.9800, 3.0, False),
        ("2021-01-01 09:20:00 UTC", 0.0000, 1.0000, 0.0, True),
        ("2021-01-01 09:50:00 UTC", 0.0001, 1.0001, 0.0, True),
        ("2021-01-01 10:20:00 UTC", 0.0000, 1.0002, 0.0, True),
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


def test_multidisk_partition_contract_is_stable(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    raw.mkdir()
    _ports(raw / "ports.csv")
    config = load_config(_config(tmp_path, raw))

    first = partition_for_mmsi(123456789, config.layout.track_partitions)
    assert first == partition_for_mmsi(123456789, config.layout.track_partitions)
    assert lane_for_partition(config, first) in config.storage.track_roots
    assert lane_for_partition(config, 0) == config.storage.track_roots[0]
    assert lane_for_partition(config, 1) == config.storage.track_roots[1]


def test_checkpoint_requires_committed_output(tmp_path: Path) -> None:
    store = CheckpointStore(tmp_path / "state.sqlite")
    output = tmp_path / "part.parquet"
    output.write_bytes(b"committed")
    store.complete(
        stage="tracks",
        task_key="0001",
        signature="abc",
        source_bytes=10,
        output_paths=[output],
        output_bytes=output.stat().st_size,
        row_count=2,
        elapsed_seconds=1.5,
    )
    assert store.is_complete("tracks", "0001", "abc", [output])

    output.unlink()
    assert not store.is_complete("tracks", "0001", "abc", [output])
    store.close()


def test_progress_uses_committed_bytes_and_calibrates_eta() -> None:
    progress = StageProgress("tracks", total_bytes=1000, total_tasks=4)
    progress.start("0000", 250, "sorting")
    assert progress.completed_bytes == 0
    assert progress.percent == 0
    assert progress.display_value({"0000": 0.4}) == 100
    assert progress.completed_bytes == 0
    assert "active=0000:sorting" in progress.render()
    assert "ETA calibrating 0/3" in progress.render()

    for index in range(3):
        progress.complete(str(index), 250, elapsed_seconds=10)
    assert progress.completed_bytes == 750
    assert progress.percent == 75
    assert "ETA calibrating" not in progress.render()


def test_end_to_end_v2_outputs_country_and_auditable_groups(tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    year = raw / "2021"
    year.mkdir(parents=True)
    _ports(raw / "ports.csv")
    _raw_track(year / "2021-01-01.csv")
    config = load_config(_config(tmp_path, raw))

    run_pipeline(config, tmp_path)

    interval_glob = str(
        config.storage.products_root
        / "trajectory_intervals"
        / "year=2021"
        / "*.parquet"
    )
    columns = {
        row[0]
        for row in duckdb.sql(
            f"DESCRIBE SELECT * FROM read_parquet('{interval_glob}')"
        ).fetchall()
    }
    assert {
        "from_port_country_or_area",
        "to_port_country_or_area",
        "from_port_call_id",
        "to_port_call_id",
        "min_speed_knots",
        "mean_speed_knots",
        "max_speed_knots",
        "track_partition_id",
    } <= columns
    assert not {
        "start_latitude",
        "start_longitude",
        "end_latitude",
        "end_longitude",
        "duration_seconds",
        "distance_km",
    } & columns

    values = duckdb.sql(
        f"""
        SELECT DISTINCT from_port_country_or_area, to_port_country_or_area
        FROM read_parquet('{interval_glob}')
        """
    ).fetchall()
    assert any("Area Alpha" in row for pair in values for row in pair if row)
    assert any("Area Beta" in row for pair in values for row in pair if row)

    groups = duckdb.read_parquet(
        str(config.storage.products_root / "ports" / "port_groups.parquet")
    )
    cross_border = groups.filter("is_cross_border").fetchone()
    assert cross_border is not None
    assert set(cross_border[groups.columns.index("member_country_or_area_names")]) == {
        "Area Alpha",
        "Area Border",
    }
    assert cross_border[groups.columns.index("country_or_area_name")] == (
        "Area Alpha; Area Border"
    )

    assert not config.storage.temp_root.exists() or not any(
        config.storage.temp_root.iterdir()
    )
