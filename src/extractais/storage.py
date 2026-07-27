from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Iterable

from extractais.config import AppConfig


GIB = 1024**3
TIB = 1024**4


def total_file_size(paths: Iterable[Path]) -> int:
    return sum(
        path.stat().st_size
        for path in paths
        if path.exists() and path.is_file()
    )


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return total_file_size(
        candidate for candidate in path.rglob("*") if candidate.is_file()
    )


def free_space_bytes(path: Path) -> int:
    path.mkdir(parents=True, exist_ok=True)
    return int(shutil.disk_usage(path).free)


def minimum_free_space_bytes(config: AppConfig) -> int:
    return int(config.runtime.minimum_free_space_gb * GIB)


def ensure_storage_budget(
    config: AppConfig,
    operation: str,
    estimated_output_bytes: int = 0,
) -> int:
    free_bytes = free_space_bytes(config.storage.work_root)
    reserve_bytes = minimum_free_space_bytes(config)
    output_bytes = max(0, int(estimated_output_bytes))
    required_bytes = reserve_bytes + output_bytes
    if free_bytes < required_bytes:
        raise RuntimeError(
            f"Storage guard stopped before {operation}: "
            f"{free_bytes / GIB:.1f} GiB available, "
            f"{reserve_bytes / GIB:.1f} GiB reserved, "
            f"{output_bytes / GIB:.1f} GiB estimated for the active work unit"
        )
    return free_bytes


def duckdb_temp_budget_bytes(
    config: AppConfig,
    output_reserve_bytes: int = 0,
) -> int:
    work_root = config.storage.work_root
    temp_root = config.storage.temp_directory
    work_root.mkdir(parents=True, exist_ok=True)
    temp_root.mkdir(parents=True, exist_ok=True)
    same_volume = os.stat(work_root).st_dev == os.stat(temp_root).st_dev
    free_bytes = free_space_bytes(temp_root)
    reserve_bytes = minimum_free_space_bytes(config)
    if same_volume:
        reserve_bytes += max(0, int(output_reserve_bytes))
    available_bytes = free_bytes - reserve_bytes
    if available_bytes <= 0:
        raise RuntimeError(
            "Storage guard stopped before opening DuckDB: "
            f"temporary volume has {free_bytes / GIB:.1f} GiB available and "
            f"requires {reserve_bytes / GIB:.1f} GiB reserved"
        )
    return available_bytes
