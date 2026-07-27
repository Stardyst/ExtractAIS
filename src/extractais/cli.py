from __future__ import annotations

import argparse
import json
import shutil
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from extractais.calls import build_port_calls
from extractais.config import AppConfig, load_config
from extractais.intervals import build_intervals
from extractais.inventory import Inventory, discover_files
from extractais.manifest import read_json, write_json_atomic
from extractais.ports import build_ports
from extractais.prepare import prepare_data
from extractais.split import split_files
from extractais.stops import build_stops
from extractais.validate import build_validation


def _paths(values: Optional[Iterable[str]]) -> Optional[list[Path]]:
    return [Path(value) for value in values] if values else None


def _inventory(config: AppConfig, explicit: Optional[list[Path]]) -> Inventory:
    inventory = discover_files(config, explicit_files=explicit)
    path = config.storage.work_root / "manifests" / "input_inventory.json"
    write_json_atomic(path, inventory.to_dict())
    return inventory


def _add_force(subparser: argparse.ArgumentParser) -> None:
    subparser.add_argument(
        "--force", action="store_true", help="rebuild completed items in this stage"
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="extractais")
    parser.add_argument("--config", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory_parser = subparsers.add_parser("inventory", help="inventory raw daily CSV files")
    inventory_parser.add_argument("--input-file", action="append")

    split_parser = subparsers.add_parser("split", help="separate dynamic and static AIS")
    split_parser.add_argument("--input-file", action="append")
    split_parser.add_argument("--limit-files", type=int)
    _add_force(split_parser)

    for name, help_text in (
        ("prepare", "compact static AIS and build sorted MMSI track buckets"),
        ("stops", "detect stationary events"),
        ("ports", "build port groups and multi-circle anchors"),
        ("calls", "match positions and confirm port calls"),
        ("intervals", "build five-state annual trajectory intervals"),
        ("validate", "build port and state quality reports"),
    ):
        stage_parser = subparsers.add_parser(name, help=help_text)
        _add_force(stage_parser)

    run_parser = subparsers.add_parser("run-all", help="run or resume the complete pipeline")
    run_parser.add_argument("--input-file", action="append")
    run_parser.add_argument("--limit-files", type=int)
    _add_force(run_parser)
    subparsers.add_parser("status", help="show checkpoint completion and output paths")
    return parser


def _stage_summary(manifest: dict[str, Any]) -> dict[str, Any]:
    items = manifest.get("items", {})
    return {
        "stage": manifest.get("stage"),
        "completed_items": sum(item.get("status") == "complete" for item in items.values()),
        "failed_items": sum(item.get("status") == "failed" for item in items.values()),
    }


def _status(config: AppConfig) -> dict[str, Any]:
    config.storage.work_root.mkdir(parents=True, exist_ok=True)
    free_space_gb = shutil.disk_usage(config.storage.work_root).free / 1024**3
    manifest_root = config.storage.work_root / "manifests"
    stages: dict[str, Any] = {}
    split = read_json(manifest_root / "split.json", {"files": {}})
    split_files_state = split.get("files", {})
    stages["split"] = {
        "completed_files": sum(
            row.get("status") == "complete" for row in split_files_state.values()
        ),
        "failed_files": sum(row.get("status") == "failed" for row in split_files_state.values()),
    }
    for name in ("prepare", "stops", "ports", "calls", "intervals", "validate"):
        manifest = read_json(manifest_root / f"{name}.json", {"stage": name, "items": {}})
        stages[name] = _stage_summary(manifest)
    return {
        "work_root": str(config.storage.work_root.resolve()),
        "free_space_gb": round(free_space_gb, 2),
        "minimum_free_space_gb": config.runtime.minimum_free_space_gb,
        "runtime_profiles": {
            "global": {
                "threads": config.runtime.threads,
                "memory_limit": config.runtime.memory_limit,
            },
            "bucket": {
                "workers": config.runtime.bucket_workers,
                "threads_per_worker": config.runtime.bucket_threads,
                "memory_limit_per_worker": (
                    config.runtime.bucket_memory_limit
                ),
                "temp_limit_gb_per_worker": (
                    config.runtime.bucket_temp_limit_gb
                ),
            },
        },
        "prepare_settings": {
            "mmsi_buckets": config.prepare.mmsi_buckets,
            "partition_write_max_open_files": (
                config.prepare.partition_write_max_open_files
            ),
            "row_group_size": config.prepare.row_group_size,
        },
        "stages": stages,
        "trajectory_intervals": str(
            (config.storage.work_root / "outputs" / "trajectory_intervals").resolve()
        ),
        "validation_reports": str(
            (config.storage.work_root / "outputs" / "validation").resolve()
        ),
        "static_vessels": str(
            (config.storage.work_root / "stage02_static" / "vessels.parquet").resolve()
        ),
    }


def _run_stage(
    function: Callable[[AppConfig, Path, bool], dict[str, Any]],
    config: AppConfig,
    force: bool,
) -> dict[str, Any]:
    return function(config, Path.cwd(), force)


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)

    if args.command == "status":
        print(json.dumps(_status(config), ensure_ascii=False, indent=2))
        return

    if args.command == "inventory":
        inventory = _inventory(config, _paths(args.input_file))
        summary = {
            "files": len(inventory.files),
            "total_size_bytes": inventory.total_size_bytes,
            "missing_dates": len(inventory.missing_dates),
            "duplicate_dates": inventory.duplicate_dates,
            "manifest": str(
                (config.storage.work_root / "manifests" / "input_inventory.json").resolve()
            ),
        }
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        return

    force = bool(getattr(args, "force", False))
    if args.command in {"split", "run-all"}:
        inventory = _inventory(config, _paths(getattr(args, "input_file", None)))
        split_manifest = split_files(
            config,
            inventory.files,
            project_root=Path.cwd(),
            force=force,
            limit_files=getattr(args, "limit_files", None),
        )
        if args.command == "split":
            completed = sum(
                record.get("status") == "complete"
                for record in split_manifest["files"].values()
            )
            print(json.dumps({"completed_files": completed}, indent=2))
            return

    stage_functions = {
        "prepare": prepare_data,
        "stops": build_stops,
        "ports": build_ports,
        "calls": build_port_calls,
        "intervals": build_intervals,
        "validate": build_validation,
    }
    if args.command in stage_functions:
        manifest = _run_stage(stage_functions[args.command], config, force)
        print(json.dumps(_stage_summary(manifest), ensure_ascii=False, indent=2))
        return

    if args.command == "run-all":
        for function in stage_functions.values():
            _run_stage(function, config, force)
        print(json.dumps(_status(config), ensure_ascii=False, indent=2))
