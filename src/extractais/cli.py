from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Iterable, Optional

from extractais.config import AppConfig, load_config
from extractais.inventory import Inventory, discover_files
from extractais.manifest import write_json_atomic
from extractais.split import split_files


def _paths(values: Optional[Iterable[str]]) -> Optional[list[Path]]:
    return [Path(value) for value in values] if values else None


def _inventory(config: AppConfig, explicit: Optional[list[Path]]) -> Inventory:
    inventory = discover_files(config, explicit_files=explicit)
    path = config.storage.work_root / "manifests" / "input_inventory.json"
    write_json_atomic(path, inventory.to_dict())
    return inventory


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="extractais")
    parser.add_argument("--config", type=Path, required=True)
    subparsers = parser.add_subparsers(dest="command", required=True)

    inventory_parser = subparsers.add_parser("inventory")
    inventory_parser.add_argument("--input-file", action="append")

    split_parser = subparsers.add_parser("split")
    split_parser.add_argument("--input-file", action="append")
    split_parser.add_argument("--limit-files", type=int)
    split_parser.add_argument("--force", action="store_true")
    return parser


def main(argv: Optional[list[str]] = None) -> None:
    args = build_parser().parse_args(argv)
    config = load_config(args.config)
    explicit = _paths(getattr(args, "input_file", None))
    inventory = _inventory(config, explicit)

    if args.command == "inventory":
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

    if args.command == "split":
        manifest = split_files(
            config,
            inventory.files,
            project_root=Path.cwd(),
            force=args.force,
            limit_files=args.limit_files,
        )
        completed = sum(
            record.get("status") == "complete" for record in manifest["files"].values()
        )
        print(json.dumps({"completed_files": completed}, indent=2))
