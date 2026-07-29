from __future__ import annotations

import os
import re
import shutil
from pathlib import Path
from typing import Iterable

from extractais.config import AppConfig


GIB = 1024**3
TIB = 1024**4


def parse_size_bytes(value: str) -> int:
    match = re.fullmatch(r"\s*(\d+(?:\.\d+)?)\s*([KMGT]?I?B)\s*", value.upper())
    if match is None:
        raise ValueError(f"Unsupported size value: {value}")
    amount = float(match.group(1))
    unit = match.group(2)
    decimal = {"B": 1, "KB": 1000, "MB": 1000**2, "GB": 1000**3, "TB": 1000**4}
    binary = {"KIB": 1024, "MIB": 1024**2, "GIB": 1024**3, "TIB": 1024**4}
    return int(amount * (decimal | binary)[unit])


def total_file_size(paths: Iterable[Path]) -> int:
    return sum(
        path.stat().st_size
        for path in paths
        if path.exists() and path.is_file()
    )


def directory_size(path: Path) -> int:
    return directory_stats(path)[0]


def directory_stats(path: Path) -> tuple[int, int]:
    if not path.exists():
        return 0, 0
    total_bytes = 0
    file_count = 0
    for candidate in path.rglob("*"):
        if candidate.is_file():
            try:
                total_bytes += candidate.stat().st_size
                file_count += 1
            except OSError:
                # Active writers may replace a file between enumeration and stat.
                continue
    return total_bytes, file_count


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


def ensure_parallel_storage_budget(
    config: AppConfig,
    operation: str,
    active_output_bytes: int,
    next_output_bytes: int,
) -> int:
    free_bytes = free_space_bytes(config.storage.work_root)
    reserve_bytes = minimum_free_space_bytes(config)
    temp_bytes = int(
        config.runtime.bucket_workers
        * config.runtime.bucket_temp_limit_gb
        * GIB
    )
    output_bytes = max(0, int(active_output_bytes)) + max(
        0, int(next_output_bytes)
    )
    required_bytes = reserve_bytes + temp_bytes + output_bytes
    if free_bytes < required_bytes:
        raise RuntimeError(
            f"Storage guard stopped before {operation}: "
            f"{free_bytes / GIB:.1f} GiB available, "
            f"{reserve_bytes / GIB:.1f} GiB reserved, "
            f"{temp_bytes / GIB:.1f} GiB reserved for concurrent DuckDB temp, "
            f"{output_bytes / GIB:.1f} GiB estimated for active outputs"
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
