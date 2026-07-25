from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable

from extractais.manifest import read_json, write_json_atomic


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def signature(values: Iterable[Any]) -> str:
    canonical = json.dumps(list(values), sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:16]


def load_stage_manifest(path: Path, stage: str) -> Dict[str, Any]:
    return read_json(path, {"version": 1, "stage": stage, "items": {}})


def save_stage_manifest(path: Path, manifest: Dict[str, Any]) -> None:
    manifest["updated_at_utc"] = utc_now()
    write_json_atomic(path, manifest)


def item_is_complete(
    manifest: Dict[str, Any],
    key: str,
    stage_hash: str,
    source_signature: str,
    outputs: Iterable[Path],
) -> bool:
    item = manifest.get("items", {}).get(key)
    return bool(
        item
        and item.get("status") == "complete"
        and item.get("config_hash") == stage_hash
        and item.get("source_signature") == source_signature
        and all(path.exists() for path in outputs)
    )
