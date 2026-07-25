from __future__ import annotations

import os
import shutil
from pathlib import Path


def temporary_file(path: Path) -> Path:
    return path.with_name(path.stem + ".tmp" + path.suffix)


def temporary_directory(path: Path) -> Path:
    return path.with_name(path.name + ".tmp")


def replace_file(temporary: Path, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(temporary, final)


def replace_directory(temporary: Path, final: Path, work_root: Path) -> None:
    _assert_within_work_root(final, work_root)
    if final.exists():
        shutil.rmtree(final)
    os.replace(temporary, final)


def remove_path(path: Path, work_root: Path) -> None:
    _assert_within_work_root(path, work_root)
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def _assert_within_work_root(path: Path, work_root: Path) -> None:
    resolved = path.resolve()
    root = work_root.resolve()
    if resolved == root or root not in resolved.parents:
        raise ValueError(f"Refusing to modify path outside work_root: {resolved}")
