from __future__ import annotations

from pathlib import Path

import pytest

from extractais.config import load_config
from extractais.pipeline import run_pipeline
from extractais.storage import ensure_space
from test_v2_contract import _config, _ports, _raw_track


def _sample(tmp_path: Path):
    raw = tmp_path / "raw"
    year = raw / "2021"
    year.mkdir(parents=True)
    _ports(raw / "ports.csv")
    _raw_track(year / "2021-01-01.csv")
    config_path = _config(tmp_path, raw)
    return config_path, load_config(config_path)


def test_full_rerun_uses_checkpoints_and_keeps_outputs(tmp_path: Path) -> None:
    _path, config = _sample(tmp_path)
    run_pipeline(config, tmp_path)
    interval = next(
        (config.storage.products_root / "trajectory_intervals").rglob("*.parquet")
    )
    geometry = next(config.storage.evidence_roots[0].rglob("*.parquet"))
    before = (interval.stat().st_mtime_ns, geometry.stat().st_mtime_ns)

    run_pipeline(config, tmp_path)

    assert (interval.stat().st_mtime_ns, geometry.stat().st_mtime_ns) == before


def test_port_group_change_reuses_geometry(tmp_path: Path) -> None:
    config_path, config = _sample(tmp_path)
    run_pipeline(config, tmp_path)
    geometry = next(
        (root / "point_anchor_candidates" / "partition=0000.parquet")
        for root in config.storage.evidence_roots
        if (root / "point_anchor_candidates" / "partition=0000.parquet").exists()
    )
    interval = config.storage.products_root / "trajectory_intervals" / "year=2021" / "partition=0000.parquet"
    before_geometry = geometry.stat().st_mtime_ns
    before_interval = interval.stat().st_mtime_ns

    text = config_path.read_text(encoding="utf-8").replace(
        "group_distance_km: 4", "group_distance_km: 0.5"
    )
    text = text.replace("excluded_harbor_sizes: []", "excluded_harbor_sizes: [Small]")
    config_path.write_text(text, encoding="utf-8")
    run_pipeline(load_config(config_path), tmp_path)

    assert geometry.stat().st_mtime_ns == before_geometry
    assert interval.stat().st_mtime_ns > before_interval


def test_config_rejects_multiple_workers_per_physical_track_root(tmp_path: Path) -> None:
    config_path, _config_value = _sample(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            "workers_per_track_root: 1", "workers_per_track_root: 2"
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="must be 1"):
        load_config(config_path)


def test_config_rejects_temp_inside_a_permanent_root(tmp_path: Path) -> None:
    config_path, config = _sample(tmp_path)
    config_path.write_text(
        config_path.read_text(encoding="utf-8").replace(
            f'temp_root: "{config.storage.temp_root.as_posix()}"',
            f'temp_root: "{(config.storage.track_roots[0] / "temp").as_posix()}"',
        ),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="must not overlap"):
        load_config(config_path)


def test_storage_guard_checks_reserve_plus_next_output(tmp_path: Path) -> None:
    free = __import__("shutil").disk_usage(tmp_path).free
    with pytest.raises(RuntimeError, match="Storage guard stopped"):
        ensure_space(tmp_path, free / 1024**3, 1, "test task")
