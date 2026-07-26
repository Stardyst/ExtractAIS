import csv
import os
from pathlib import Path

import duckdb
import pytest

import extractais.split as split_module
from extractais.config import load_config
from extractais.inventory import discover_files
from extractais.split import split_files
from test_inventory import _write_config


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


def test_split_separates_dynamic_static_and_counts_invalid(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    csv_path = year / "2021-01-01.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerow(_row(
            timestamp="2021-01-01 01:02:03 UTC", MMSI="123456789", msg_type="1",
            latitude="10", longitude="20", speed="102.3", course="360",
            heading="511", collection_type="TERRESTRIAL",
            geopoint_index_id="['83196bfffffffff']", at_dock="True",
        ))
        writer.writerow(_row(
            timestamp="2021-01-01 02:02:03 UTC", MMSI="123456789", msg_type="5",
            IMO="9876543", ship_type="70", length="200", width="30",
        ))
        writer.writerow(_row(
            timestamp="bad", MMSI="123456789", msg_type="1",
            latitude="10", longitude="20",
        ))
        writer.writerow(_row(
            timestamp="2021-01-01 03:02:03 UTC", MMSI="123456789", msg_type="4",
        ))

    config = load_config(_write_config(tmp_path, raw_root, tmp_path / "work"))
    inventory = discover_files(config)
    manifest = split_files(config, inventory.files, tmp_path)
    record = manifest["files"][str(csv_path.resolve())]

    assert record["dynamic_message_rows"] == 2
    assert record["dynamic_valid_rows"] == 1
    assert record["dynamic_invalid_rows"] == 1
    assert record["static_message_rows"] == 1
    assert record["static_valid_rows"] == 1
    assert record["other_rows"] == 1
    assert record["dynamic_output_bytes"] == Path(record["dynamic_path"]).stat().st_size
    assert record["static_output_bytes"] == Path(record["static_path"]).stat().st_size
    assert record["total_output_bytes"] == (
        record["dynamic_output_bytes"] + record["static_output_bytes"]
    )
    assert record["compression_ratio"] > 0
    assert record["free_space_bytes_after"] > 0

    dynamic = duckdb.read_parquet(record["dynamic_path"]).fetchone()
    assert dynamic[0].isoformat(sep=" ") == "2021-01-01 01:02:03"
    assert dynamic[1] == 123456789
    assert dynamic[5] is None
    assert dynamic[6] is None
    assert dynamic[7] is None
    assert dynamic[12] == "terrestrial"
    assert dynamic[17] == "83196bfffffffff"

    static = duckdb.read_parquet(record["static_path"]).fetchone()
    assert static[1] == 123456789
    assert static[3] == 9876543


def test_split_runs_each_day_in_a_distinct_worker_process(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    for day in (1, 2):
        csv_path = year / f"2021-01-{day:02d}.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow(COLUMNS)
            writer.writerow(
                _row(
                    timestamp=f"2021-01-{day:02d} 00:00:00 UTC",
                    MMSI="123456789",
                    msg_type="1",
                    latitude="10",
                    longitude="20",
                    speed="5",
                )
            )

    config = load_config(_write_config(tmp_path, raw_root, tmp_path / "work"))
    inventory = discover_files(config)
    manifest = split_files(config, inventory.files, tmp_path)
    worker_process_ids = {
        record["worker_process_id"] for record in manifest["files"].values()
    }

    assert len(worker_process_ids) == 2
    assert os.getpid() not in worker_process_ids


def test_split_stops_before_reading_when_free_space_guard_is_hit(
    tmp_path: Path, monkeypatch
) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    csv_path = year / "2021-01-01.csv"
    with csv_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(COLUMNS)
        writer.writerow(
            _row(
                timestamp="2021-01-01 00:00:00 UTC",
                MMSI="123456789",
                msg_type="1",
                latitude="10",
                longitude="20",
            )
        )

    config_path = _write_config(tmp_path, raw_root, tmp_path / "work")
    config_text = config_path.read_text(encoding="utf-8").replace(
        "progress_bar_time_ms: 1000",
        "progress_bar_time_ms: 1000\n  minimum_free_space_gb: 1",
    )
    config_path.write_text(config_text, encoding="utf-8")
    config = load_config(config_path)
    inventory = discover_files(config)
    monkeypatch.setattr(split_module, "_free_space_bytes", lambda unused: 0)

    with pytest.raises(RuntimeError, match="Free-space guard stopped"):
        split_files(config, inventory.files, tmp_path)

    assert not (tmp_path / "work" / "stage01_split").exists()
