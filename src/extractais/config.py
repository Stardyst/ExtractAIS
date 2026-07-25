from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import yaml


@dataclass(frozen=True)
class InputConfig:
    raw_root: Path
    year_directories: Dict[int, str]
    filename_pattern: str
    ports_csv: Path


@dataclass(frozen=True)
class StorageConfig:
    work_root: Path
    temp_directory: Path


@dataclass(frozen=True)
class RuntimeConfig:
    threads: int
    memory_limit: str
    enable_progress: bool
    progress_bar_time_ms: int


@dataclass(frozen=True)
class SplitConfig:
    dynamic_message_types: List[int]
    static_message_types: List[int]
    compression: str
    row_group_size: int


@dataclass(frozen=True)
class AppConfig:
    input: InputConfig
    storage: StorageConfig
    runtime: RuntimeConfig
    split: SplitConfig
    source_path: Path
    config_hash: str


def _resolve_path(value: str, base: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (base / path).resolve()


def load_config(path: Path) -> AppConfig:
    source_path = path.resolve()
    with source_path.open("r", encoding="utf-8") as handle:
        raw = yaml.safe_load(handle)

    canonical = json.dumps(raw, sort_keys=True, separators=(",", ":"))
    config_hash = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]
    base = source_path.parent

    input_raw = raw["input"]
    storage_raw = raw["storage"]
    runtime_raw = raw["runtime"]
    split_raw = raw["split"]

    raw_root = _resolve_path(input_raw["raw_root"], base)
    return AppConfig(
        input=InputConfig(
            raw_root=raw_root,
            year_directories={
                int(year): str(directory)
                for year, directory in input_raw["year_directories"].items()
            },
            filename_pattern=str(input_raw.get("filename_pattern", "*.csv")),
            ports_csv=_resolve_path(input_raw["ports_csv"], base),
        ),
        storage=StorageConfig(
            work_root=_resolve_path(storage_raw["work_root"], base),
            temp_directory=_resolve_path(storage_raw["temp_directory"], base),
        ),
        runtime=RuntimeConfig(
            threads=int(runtime_raw["threads"]),
            memory_limit=str(runtime_raw["memory_limit"]),
            enable_progress=bool(runtime_raw.get("enable_progress", True)),
            progress_bar_time_ms=int(runtime_raw.get("progress_bar_time_ms", 2000)),
        ),
        split=SplitConfig(
            dynamic_message_types=[int(value) for value in split_raw["dynamic_message_types"]],
            static_message_types=[int(value) for value in split_raw["static_message_types"]],
            compression=str(split_raw.get("compression", "zstd")).lower(),
            row_group_size=int(split_raw.get("row_group_size", 1_000_000)),
        ),
        source_path=source_path,
        config_hash=config_hash,
    )
