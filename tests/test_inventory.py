from pathlib import Path

from extractais.config import load_config
from extractais.inventory import discover_files


def _write_config(path: Path, raw_root: Path, work_root: Path) -> Path:
    config = path / "config.yaml"
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
  enable_progress: false
  minimum_free_space_gb: 0
split:
  dynamic_message_types: [1, 2, 3, 18, 19, 27]
  static_message_types: [5, 24]
  compression: "zstd"
  row_group_size: 10000
""".strip(),
        encoding="utf-8",
    )
    return config


def test_inventory_discovers_and_orders_daily_files(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    year = raw_root / "2021"
    year.mkdir(parents=True)
    (year / "2021-01-02.csv").write_text("x\n", encoding="utf-8")
    (year / "2021-01-01.csv").write_text("x\n", encoding="utf-8")
    config = load_config(_write_config(tmp_path, raw_root, tmp_path / "work"))

    inventory = discover_files(config)

    assert [item.date for item in inventory.files] == ["2021-01-01", "2021-01-02"]
    assert inventory.duplicate_dates == []
    assert "2021-01-03" in inventory.missing_dates


def test_inventory_accepts_dated_sample_filename_as_explicit_input(tmp_path: Path) -> None:
    raw_root = tmp_path / "raw"
    raw_root.mkdir()
    sample = raw_root / "sample_2021-01-23_1100.csv"
    sample.write_text("x\n", encoding="utf-8")
    config = load_config(_write_config(tmp_path, raw_root, tmp_path / "work"))

    inventory = discover_files(config, explicit_files=[sample])

    assert len(inventory.files) == 1
    assert inventory.files[0].date == "2021-01-23"
