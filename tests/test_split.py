import csv
from pathlib import Path

import duckdb

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
