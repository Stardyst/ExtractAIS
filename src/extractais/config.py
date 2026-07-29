from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml


@dataclass(frozen=True)
class InputConfig:
    raw_root: Path
    year_directories: dict[int, str]
    filename_pattern: str
    ports_csv: Path


@dataclass(frozen=True)
class StorageConfig:
    track_roots: tuple[Path, ...]
    temp_root: Path
    products_root: Path
    evidence_roots: tuple[Path, ...]
    reserves_gib: dict[str, float]


@dataclass(frozen=True)
class RuntimeConfig:
    global_threads: int
    global_memory: str
    workers_per_track_root: int
    worker_threads: int
    worker_memory: str
    worker_temp_gib: float
    progress_interval_seconds: float
    enable_progress: bool

    @property
    def worker_count(self) -> int:
        return self.workers_per_track_root


@dataclass(frozen=True)
class LayoutConfig:
    track_partitions: int
    row_group_size: int
    compression: str


@dataclass(frozen=True)
class CleaningConfig:
    dynamic_message_types: tuple[int, ...]
    static_message_types: tuple[int, ...]
    max_implied_speed_knots: float


@dataclass(frozen=True)
class StopConfig:
    max_speed_knots: float
    max_step_km: float
    max_point_gap_minutes: int
    min_duration_minutes: int
    max_diameter_km: float


@dataclass(frozen=True)
class PortConfig:
    entry_radius_km: float
    exit_radius_km: float
    approach_radius_km: float
    group_distance_km: float
    anchor_assignment_radius_km: float
    anchor_grid_degrees: float
    anchor_min_stop_events: int
    anchor_min_vessels: int
    call_min_points: int
    call_max_point_gap_hours: float
    ambiguity_margin_km: float
    excluded_harbor_sizes: tuple[str, ...] = field(default_factory=tuple)


@dataclass(frozen=True)
class IntervalConfig:
    unknown_gap_hours: float


@dataclass(frozen=True)
class AppConfig:
    input: InputConfig
    storage: StorageConfig
    runtime: RuntimeConfig
    layout: LayoutConfig
    cleaning: CleaningConfig
    stops: StopConfig
    ports: PortConfig
    intervals: IntervalConfig
    source_path: Path
    raw: dict[str, Any]

    def section_hash(self, *sections: str) -> str:
        value = {name: self.raw.get(name, {}) for name in sections}
        canonical = json.dumps(value, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:20]


def _path(value: str, base: Path) -> Path:
    candidate = Path(value).expanduser()
    return candidate.resolve() if candidate.is_absolute() else (base / candidate).resolve()


def _positive_int(raw: dict[str, Any], name: str, default: int) -> int:
    value = int(raw.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive")
    return value


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
    layout_raw = raw["layout"]
    cleaning_raw = raw["cleaning"]
    stop_raw = raw["stops"]
    port_raw = raw["ports"]
    interval_raw = raw["intervals"]
    reserves = storage_raw.get("reserves_gib", {})

    config = AppConfig(
        input=InputConfig(
            raw_root=_path(input_raw["raw_root"], base),
            year_directories={
                int(year): str(directory)
                for year, directory in input_raw["year_directories"].items()
            },
            filename_pattern=str(input_raw.get("filename_pattern", "*.csv")),
            ports_csv=_path(input_raw["ports_csv"], base),
        ),
        storage=StorageConfig(
            track_roots=tuple(_path(value, base) for value in storage_raw["track_roots"]),
            temp_root=_path(storage_raw["temp_root"], base),
            products_root=_path(storage_raw["products_root"], base),
            evidence_roots=tuple(
                _path(value, base) for value in storage_raw["evidence_roots"]
            ),
            reserves_gib={
                name: float(reserves.get(name, 0.0))
                for name in ("tracks", "temp", "products", "evidence")
            },
        ),
        runtime=RuntimeConfig(
            global_threads=_positive_int(runtime_raw, "global_threads", 16),
            global_memory=str(runtime_raw.get("global_memory", "90GB")),
            workers_per_track_root=_positive_int(
                runtime_raw, "workers_per_track_root", 1
            ),
            worker_threads=_positive_int(runtime_raw, "worker_threads", 6),
            worker_memory=str(runtime_raw.get("worker_memory", "40GB")),
            worker_temp_gib=float(runtime_raw.get("worker_temp_gib", 180)),
            progress_interval_seconds=float(
                runtime_raw.get("progress_interval_seconds", 5)
            ),
            enable_progress=bool(runtime_raw.get("enable_progress", True)),
        ),
        layout=LayoutConfig(
            track_partitions=_positive_int(layout_raw, "track_partitions", 1024),
            row_group_size=_positive_int(layout_raw, "row_group_size", 50_000),
            compression=str(layout_raw.get("compression", "zstd")).lower(),
        ),
        cleaning=CleaningConfig(
            dynamic_message_types=tuple(
                int(value) for value in cleaning_raw["dynamic_message_types"]
            ),
            static_message_types=tuple(
                int(value) for value in cleaning_raw["static_message_types"]
            ),
            max_implied_speed_knots=float(
                cleaning_raw.get("max_implied_speed_knots", 80)
            ),
        ),
        stops=StopConfig(
            max_speed_knots=float(stop_raw["max_speed_knots"]),
            max_step_km=float(stop_raw["max_step_km"]),
            max_point_gap_minutes=int(stop_raw["max_point_gap_minutes"]),
            min_duration_minutes=int(stop_raw["min_duration_minutes"]),
            max_diameter_km=float(stop_raw["max_diameter_km"]),
        ),
        ports=PortConfig(
            entry_radius_km=float(port_raw["entry_radius_km"]),
            exit_radius_km=float(port_raw["exit_radius_km"]),
            approach_radius_km=float(port_raw["approach_radius_km"]),
            group_distance_km=float(port_raw["group_distance_km"]),
            anchor_assignment_radius_km=float(port_raw["anchor_assignment_radius_km"]),
            anchor_grid_degrees=float(port_raw["anchor_grid_degrees"]),
            anchor_min_stop_events=int(port_raw["anchor_min_stop_events"]),
            anchor_min_vessels=int(port_raw["anchor_min_vessels"]),
            call_min_points=int(port_raw["call_min_points"]),
            call_max_point_gap_hours=float(port_raw["call_max_point_gap_hours"]),
            ambiguity_margin_km=float(port_raw["ambiguity_margin_km"]),
            excluded_harbor_sizes=tuple(
                str(value).strip()
                for value in port_raw.get("excluded_harbor_sizes", [])
            ),
        ),
        intervals=IntervalConfig(
            unknown_gap_hours=float(interval_raw["unknown_gap_hours"])
        ),
        source_path=source_path,
        raw=raw,
    )
    _validate(config)
    return config


def _validate(config: AppConfig) -> None:
    if len(config.storage.track_roots) < 2:
        raise ValueError("storage.track_roots must contain at least two roots")
    if not config.storage.evidence_roots:
        raise ValueError("storage.evidence_roots cannot be empty")
    if len(set(config.storage.track_roots)) != len(config.storage.track_roots):
        raise ValueError("storage.track_roots must be distinct")
    if config.layout.track_partitions & (config.layout.track_partitions - 1):
        raise ValueError("layout.track_partitions must be a power of two")
    if config.layout.track_partitions % len(config.storage.track_roots):
        raise ValueError("track partitions must divide evenly across track roots")
    if config.runtime.worker_temp_gib <= 0:
        raise ValueError("runtime.worker_temp_gib must be positive")
    if config.runtime.workers_per_track_root != 1:
        raise ValueError(
            "runtime.workers_per_track_root must be 1; each physical track disk "
            "runs one sequential workload"
        )
    if config.runtime.progress_interval_seconds <= 0:
        raise ValueError("runtime.progress_interval_seconds must be positive")
    if any(value < 0 for value in config.storage.reserves_gib.values()):
        raise ValueError("storage reserves cannot be negative")
    if not (
        0 < config.ports.entry_radius_km
        <= config.ports.exit_radius_km
        <= config.ports.approach_radius_km
    ):
        raise ValueError("port radii must satisfy entry <= exit <= approach")
    permanent = set(config.storage.track_roots) | {
        config.storage.products_root,
        *config.storage.evidence_roots,
    }
    permanent_paths = sorted(permanent, key=str)

    def overlaps(left: Path, right: Path) -> bool:
        return left == right or left in right.parents or right in left.parents

    for index, left in enumerate(permanent_paths):
        for right in permanent_paths[index + 1 :]:
            if overlaps(left, right):
                raise ValueError(
                    f"permanent output roots must not overlap: {left} and {right}"
                )
    temp = config.storage.temp_root
    if temp == Path(temp.anchor):
        raise ValueError("storage.temp_root cannot be a filesystem root")
    protected = [config.input.raw_root, *permanent_paths]
    for path in protected:
        if overlaps(temp, path):
            raise ValueError(
                f"storage.temp_root must not overlap protected path: {path}"
            )
