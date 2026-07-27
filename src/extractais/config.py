from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List

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
    bucket_workers: int
    bucket_threads: int
    bucket_memory_limit: str
    bucket_temp_limit_gb: float
    enable_progress: bool
    minimum_free_space_gb: float


@dataclass(frozen=True)
class SplitConfig:
    dynamic_message_types: List[int]
    static_message_types: List[int]
    compression: str
    row_group_size: int


@dataclass(frozen=True)
class PrepareConfig:
    mmsi_buckets: int = 256
    partition_write_max_open_files: int = 100
    compression: str = "zstd"
    row_group_size: int = 250_000
    max_implied_speed_knots: float = 80.0


@dataclass(frozen=True)
class StopConfig:
    max_speed_knots: float = 0.5
    max_step_km: float = 0.25
    max_point_gap_minutes: int = 60
    min_duration_minutes: int = 60
    max_diameter_km: float = 1.0


@dataclass(frozen=True)
class PortConfig:
    entry_radius_km: float = 3.0
    exit_radius_km: float = 4.0
    approach_radius_km: float = 10.0
    group_distance_km: float = 4.0
    anchor_assignment_radius_km: float = 20.0
    anchor_grid_degrees: float = 0.005
    anchor_min_stop_events: int = 3
    anchor_min_vessels: int = 3
    call_min_points: int = 2
    call_max_point_gap_hours: float = 6.0
    ambiguity_margin_km: float = 1.0
    excluded_harbor_sizes: List[str] = field(default_factory=list)


@dataclass(frozen=True)
class IntervalConfig:
    unknown_gap_hours: float = 6.0


@dataclass(frozen=True)
class AppConfig:
    input: InputConfig
    storage: StorageConfig
    runtime: RuntimeConfig
    split: SplitConfig
    prepare: PrepareConfig
    stops: StopConfig
    ports: PortConfig
    intervals: IntervalConfig
    source_path: Path
    raw: Dict[str, Any]

    @property
    def config_hash(self) -> str:
        return _hash_value(self.raw)

    def stage_hash(self, *sections: str) -> str:
        selected = {name: self.raw.get(name, {}) for name in sections}
        return _hash_value(selected)


def _hash_value(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def _resolve_path(value: str, base: Path) -> Path:
    path = Path(value).expanduser()
    return path if path.is_absolute() else (base / path).resolve()


def load_config(path: Path) -> AppConfig:
    source_path = path.resolve()
    with source_path.open("r", encoding="utf-8-sig") as handle:
        raw = yaml.safe_load(handle)
    if not isinstance(raw, dict):
        raise ValueError("Configuration root must be a mapping")

    base = source_path.parent
    input_raw = raw["input"]
    storage_raw = raw["storage"]
    runtime_raw = raw["runtime"]
    split_raw = raw["split"]
    prepare_raw = raw.get("prepare", {})
    stop_raw = raw.get("stops", {})
    port_raw = raw.get("ports", {})
    interval_raw = raw.get("intervals", {})

    config = AppConfig(
        input=InputConfig(
            raw_root=_resolve_path(input_raw["raw_root"], base),
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
            threads=int(runtime_raw.get("threads", 20)),
            memory_limit=str(runtime_raw.get("memory_limit", "90GB")),
            bucket_workers=int(runtime_raw.get("bucket_workers", 2)),
            bucket_threads=int(runtime_raw.get("bucket_threads", 10)),
            bucket_memory_limit=str(
                runtime_raw.get("bucket_memory_limit", "42GB")
            ),
            bucket_temp_limit_gb=float(
                runtime_raw.get("bucket_temp_limit_gb", 256)
            ),
            enable_progress=bool(runtime_raw.get("enable_progress", True)),
            minimum_free_space_gb=float(
                runtime_raw.get("minimum_free_space_gb", 500)
            ),
        ),
        split=SplitConfig(
            dynamic_message_types=[int(value) for value in split_raw["dynamic_message_types"]],
            static_message_types=[int(value) for value in split_raw["static_message_types"]],
            compression=str(split_raw.get("compression", "zstd")).lower(),
            row_group_size=int(split_raw.get("row_group_size", 1_000_000)),
        ),
        prepare=PrepareConfig(
            mmsi_buckets=int(prepare_raw.get("mmsi_buckets", 256)),
            partition_write_max_open_files=int(
                prepare_raw.get("partition_write_max_open_files", 100)
            ),
            compression=str(prepare_raw.get("compression", "zstd")).lower(),
            row_group_size=int(prepare_raw.get("row_group_size", 250_000)),
            max_implied_speed_knots=float(
                prepare_raw.get("max_implied_speed_knots", 80.0)
            ),
        ),
        stops=StopConfig(
            max_speed_knots=float(stop_raw.get("max_speed_knots", 0.5)),
            max_step_km=float(stop_raw.get("max_step_km", 0.25)),
            max_point_gap_minutes=int(stop_raw.get("max_point_gap_minutes", 60)),
            min_duration_minutes=int(stop_raw.get("min_duration_minutes", 60)),
            max_diameter_km=float(stop_raw.get("max_diameter_km", 1.0)),
        ),
        ports=PortConfig(
            entry_radius_km=float(port_raw.get("entry_radius_km", 3.0)),
            exit_radius_km=float(port_raw.get("exit_radius_km", 4.0)),
            approach_radius_km=float(port_raw.get("approach_radius_km", 10.0)),
            group_distance_km=float(port_raw.get("group_distance_km", 4.0)),
            anchor_assignment_radius_km=float(
                port_raw.get("anchor_assignment_radius_km", 20.0)
            ),
            anchor_grid_degrees=float(port_raw.get("anchor_grid_degrees", 0.005)),
            anchor_min_stop_events=int(port_raw.get("anchor_min_stop_events", 3)),
            anchor_min_vessels=int(port_raw.get("anchor_min_vessels", 3)),
            call_min_points=int(port_raw.get("call_min_points", 2)),
            call_max_point_gap_hours=float(
                port_raw.get("call_max_point_gap_hours", 6.0)
            ),
            ambiguity_margin_km=float(port_raw.get("ambiguity_margin_km", 1.0)),
            excluded_harbor_sizes=[
                str(value).strip() for value in port_raw.get("excluded_harbor_sizes", [])
            ],
        ),
        intervals=IntervalConfig(
            unknown_gap_hours=float(interval_raw.get("unknown_gap_hours", 6.0))
        ),
        source_path=source_path,
        raw=raw,
    )
    _validate_config(config)
    return config


def _validate_config(config: AppConfig) -> None:
    if config.runtime.threads <= 0:
        raise ValueError("runtime.threads must be positive")
    if config.runtime.bucket_workers <= 0:
        raise ValueError("runtime.bucket_workers must be positive")
    if config.runtime.bucket_threads <= 0:
        raise ValueError("runtime.bucket_threads must be positive")
    if config.runtime.bucket_temp_limit_gb <= 0:
        raise ValueError("runtime.bucket_temp_limit_gb must be positive")
    if config.runtime.minimum_free_space_gb < 0:
        raise ValueError("runtime.minimum_free_space_gb cannot be negative")
    if config.prepare.mmsi_buckets <= 0:
        raise ValueError("prepare.mmsi_buckets must be positive")
    if config.prepare.partition_write_max_open_files <= 0:
        raise ValueError(
            "prepare.partition_write_max_open_files must be positive"
        )
    if (
        config.prepare.partition_write_max_open_files
        > config.prepare.mmsi_buckets
    ):
        raise ValueError(
            "prepare.partition_write_max_open_files cannot exceed "
            "prepare.mmsi_buckets"
        )
    if not (
        0 < config.ports.entry_radius_km
        <= config.ports.exit_radius_km
        <= config.ports.approach_radius_km
    ):
        raise ValueError(
            "Port radii must satisfy 0 < entry_radius_km <= exit_radius_km "
            "<= approach_radius_km"
        )
    if config.ports.anchor_assignment_radius_km <= 0:
        raise ValueError("ports.anchor_assignment_radius_km must be positive")
    if config.ports.anchor_grid_degrees <= 0:
        raise ValueError("ports.anchor_grid_degrees must be positive")
    if config.stops.max_point_gap_minutes <= 0 or config.stops.min_duration_minutes <= 0:
        raise ValueError("Stop time thresholds must be positive")
    if config.intervals.unknown_gap_hours <= 0:
        raise ValueError("intervals.unknown_gap_hours must be positive")
