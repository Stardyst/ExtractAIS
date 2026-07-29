from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable

from extractais.config import AppConfig


GIB = 1024**3
TIB = 1024**4


def partition_for_mmsi(mmsi: int, partition_count: int) -> int:
    mixed = (int(mmsi) * 1_103_515_245 + 12_345) % 2_147_483_647
    return mixed & (partition_count - 1)


def partition_sql(column: str, partition_count: int) -> str:
    return (
        f"cast((((cast({column} AS BIGINT) * 1103515245 + 12345) "
        f"% 2147483647) & {partition_count - 1}) AS INTEGER)"
    )


def lane_for_partition(config: AppConfig, partition: int) -> Path:
    return config.storage.track_roots[partition % len(config.storage.track_roots)]


def evidence_root_for_partition(config: AppConfig, partition: int) -> Path:
    roots = config.storage.evidence_roots
    return roots[partition % len(roots)]


def track_path(config: AppConfig, partition: int) -> Path:
    return lane_for_partition(config, partition) / "canonical_tracks" / f"partition={partition:04d}.parquet"


def stop_path(config: AppConfig, partition: int) -> Path:
    return config.storage.products_root / "stop_events" / f"partition={partition:04d}.parquet"


def candidate_path(config: AppConfig, partition: int) -> Path:
    return evidence_root_for_partition(config, partition) / "point_anchor_candidates" / f"partition={partition:04d}.parquet"


def port_context_path(config: AppConfig, partition: int) -> Path:
    return evidence_root_for_partition(config, partition) / "port_context" / f"partition={partition:04d}.parquet"


def port_call_path(config: AppConfig, partition: int) -> Path:
    return config.storage.products_root / "port_calls" / f"partition={partition:04d}.parquet"


def total_file_size(paths: Iterable[Path]) -> int:
    return sum(path.stat().st_size for path in paths if path.exists() and path.is_file())


def directory_size(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def free_space_bytes(path: Path) -> int:
    probe = path
    while not probe.exists() and probe.parent != probe:
        probe = probe.parent
    return shutil.disk_usage(probe).free


def ensure_space(path: Path, reserve_gib: float, required_bytes: int, label: str) -> int:
    free = free_space_bytes(path)
    required = int(reserve_gib * GIB) + max(0, int(required_bytes))
    if free < required:
        raise RuntimeError(
            f"Storage guard stopped {label}: {free / GIB:.2f} GiB free at {path}, "
            f"{required / GIB:.2f} GiB required"
        )
    return free


def shared_temp_requirement(config: AppConfig) -> int:
    """Reserve spill capacity for every worker that can run concurrently."""
    return int(
        config.runtime.worker_temp_gib
        * max(1, len(config.storage.track_roots))
        * GIB
    )


def shared_output_requirement(config: AppConfig, per_task_bytes: int) -> int:
    """Protect a shared output root against concurrent task starts."""
    return max(0, int(per_task_bytes)) * max(1, len(config.storage.track_roots))
