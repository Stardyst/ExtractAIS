from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Callable, Optional

from extractais import __version__
from extractais.calls import build_calls
from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig, load_config
from extractais.geometry import build_geometry
from extractais.ingest import ingest
from extractais.intervals import build_intervals
from extractais.pipeline import checkpoint_path, inventory_signature, run_pipeline
from extractais.ports import build_ports
from extractais.runtime import write_json_atomic
from extractais.storage import GIB, directory_size, free_space_bytes
from extractais.tracks import build_tracks, cleanup_ingest_runs, tracks_are_complete
from extractais.validate import build_validation


STAGES = (
    "ingest",
    "tracks",
    "anchor_cells",
    "anchors",
    "port_groups",
    "geometry",
    "calls",
    "intervals",
    "validation_partials",
    "validation",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="extractais",
        description="ExtractAIS v2: resumable multi-disk AIS trajectory processing",
    )
    parser.add_argument("--config", required=True, type=Path)
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("inventory", help="scan raw daily CSV files without processing")
    commands.add_parser("ingest", help="normalize each raw file once into sorted lane runs")
    commands.add_parser("tracks", help="build canonical tracks, vessels, and stop events")
    commands.add_parser("ports", help="build multi-circle anchors and independent port groups")
    commands.add_parser("geometry", help="calculate compact point-to-port geometry evidence")
    commands.add_parser("calls", help="confirm port calls from geometry and port groups")
    commands.add_parser("intervals", help="build annual five-state trajectory intervals")
    commands.add_parser("validate", help="build accuracy evidence and quality summaries")
    commands.add_parser("run-all", help="run or resume the complete v2 pipeline")
    status = commands.add_parser("status", help="show checkpoints, storage, and output paths")
    status.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    commands.add_parser(
        "cleanup-staging",
        help="remove ingest runs only after all canonical track partitions are complete",
    )
    return parser


def _inventory(config: AppConfig):
    inventory, inventory_hash = inventory_signature(config)
    write_json_atomic(
        config.storage.products_root / "metadata" / "input_inventory.json",
        inventory.to_dict(),
    )
    return inventory, inventory_hash


def _status(config: AppConfig) -> dict[str, object]:
    state = checkpoint_path(config)
    counts: dict[str, int] = {stage: 0 for stage in STAGES}
    if state.exists():
        with CheckpointStore(state) as store:
            counts = {stage: len(store.completed(stage)) for stage in STAGES}
    roots = {
        "track": config.storage.track_roots,
        "temp": (config.storage.temp_root,),
        "products": (config.storage.products_root,),
        "evidence": config.storage.evidence_roots,
    }
    storage = []
    for role, paths in roots.items():
        for path in paths:
            try:
                free_gib: float | None = round(free_space_bytes(path) / GIB, 2)
                availability = "available"
            except OSError:
                free_gib = None
                availability = "unavailable"
            storage.append(
                {
                    "role": role,
                    "path": str(path),
                    "availability": availability,
                    "free_gib": free_gib,
                    "used_by_pipeline_gib": round(directory_size(path) / GIB, 2),
                    "reserve_gib": config.storage.reserves_gib[role if role != "track" else "tracks"],
                }
            )
    return {
        "pipeline_version": __version__,
        "track_partitions": config.layout.track_partitions,
        "checkpoints": counts,
        "storage": storage,
        "outputs": {
            "trajectory_intervals": str(config.storage.products_root / "trajectory_intervals"),
            "port_calls": str(config.storage.products_root / "port_calls"),
            "validation": str(config.storage.products_root / "validation"),
            "state": str(state),
        },
    }


def _print_status(status: dict[str, object], as_json: bool) -> None:
    if as_json:
        print(json.dumps(status, ensure_ascii=False, indent=2))
        return
    print(f"ExtractAIS {__version__} status")
    print(f"Track partitions: {status['track_partitions']}")
    print("Checkpoints:")
    for stage, count in status["checkpoints"].items():
        print(f"  {stage:20s} {count}")
    print("Storage:")
    for row in status["storage"]:
        free = (
            f"{row['free_gib']:10.2f} GiB"
            if row["free_gib"] is not None
            else "unavailable   "
        )
        print(
            f"  {row['role']:8s} free={free} "
            f"used={row['used_by_pipeline_gib']:10.2f} GiB  {row['path']}"
        )
    print("Outputs:")
    for name, path in status["outputs"].items():
        print(f"  {name:20s} {path}")


def _with_store(config: AppConfig, function: Callable[[CheckpointStore], None]) -> None:
    with CheckpointStore(checkpoint_path(config)) as store:
        function(store)


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)

    if args.command == "status":
        _print_status(_status(config), args.json)
        return
    if args.command == "inventory":
        inventory, inventory_hash = _inventory(config)
        print(
            json.dumps(
                {
                    "files": len(inventory.files),
                    "total_gib": round(inventory.total_size_bytes / GIB, 2),
                    "missing_dates": len(inventory.missing_dates),
                    "duplicate_dates": inventory.duplicate_dates,
                    "signature": inventory_hash,
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return
    if args.command == "run-all":
        run_pipeline(config, Path.cwd())
        _print_status(_status(config), False)
        return

    inventory, inventory_hash = _inventory(config)
    if args.command == "ingest":
        _with_store(config, lambda store: ingest(config, inventory, store))
    elif args.command == "tracks":
        def tracks_stage(store: CheckpointStore) -> None:
            if not tracks_are_complete(config, inventory_hash, store):
                ingest(config, inventory, store)
                build_tracks(config, inventory, inventory_hash, store)
            cleanup_ingest_runs(config)
        _with_store(config, tracks_stage)
    elif args.command == "ports":
        _with_store(config, lambda store: build_ports(config, store))
    elif args.command == "geometry":
        _with_store(config, lambda store: build_geometry(config, store))
    elif args.command == "calls":
        _with_store(config, lambda store: build_calls(config, store))
    elif args.command == "intervals":
        _with_store(config, lambda store: build_intervals(config, store))
    elif args.command == "validate":
        _with_store(config, lambda store: build_validation(config, store))
    elif args.command == "cleanup-staging":
        with CheckpointStore(checkpoint_path(config)) as store:
            if not tracks_are_complete(config, inventory_hash, store):
                raise RuntimeError("Refusing cleanup: canonical track partitions are incomplete")
        cleanup_ingest_runs(config)
    else:
        raise AssertionError(args.command)

    _print_status(_status(config), False)
