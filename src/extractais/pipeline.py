from __future__ import annotations

import shutil
from pathlib import Path

from extractais.calls import build_calls
from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.geometry import build_geometry
from extractais.ingest import ingest
from extractais.intervals import build_intervals
from extractais.inventory import discover_files
from extractais.ports import build_ports
from extractais.runtime import signature, write_json_atomic
from extractais.tracks import build_tracks, cleanup_ingest_runs, tracks_are_complete
from extractais.validate import build_validation


def inventory_signature(config: AppConfig) -> tuple[object, str]:
    inventory = discover_files(config)
    identity = [
        (item.path, item.size_bytes, item.modified_ns) for item in inventory.files
    ]
    return inventory, signature(identity)


def checkpoint_path(config: AppConfig) -> Path:
    return config.storage.products_root / "metadata" / "state.sqlite"


def run_pipeline(config: AppConfig, project_root: Path) -> None:
    del project_root
    inventory, inventory_hash = inventory_signature(config)
    config.storage.products_root.mkdir(parents=True, exist_ok=True)
    write_json_atomic(
        config.storage.products_root / "metadata" / "input_inventory.json",
        inventory.to_dict(),
    )
    with CheckpointStore(checkpoint_path(config)) as store:
        if not tracks_are_complete(config, inventory_hash, store):
            ingest(config, inventory, store)
            build_tracks(config, inventory, inventory_hash, store)
        cleanup_ingest_runs(config)
        build_ports(config, store)
        build_geometry(config, store)
        build_calls(config, store)
        build_intervals(config, store)
        build_validation(config, store)
    shutil.rmtree(config.storage.temp_root, ignore_errors=True)
