from __future__ import annotations

import hashlib
import json
import os
import shutil
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable

from tqdm import tqdm

from extractais.checkpoints import CheckpointStore
from extractais.config import AppConfig
from extractais.isolated import IsolatedCall, run_isolated_many
from extractais.progress import StageProgress


def signature(value: Any) -> str:
    canonical = json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]


def atomic_replace(temporary: Path, final: Path) -> None:
    final.parent.mkdir(parents=True, exist_ok=True)
    os.replace(temporary, final)


def remove_tree(path: Path, root: Path) -> None:
    resolved = path.resolve()
    resolved_root = root.resolve()
    if resolved == resolved_root or resolved_root not in resolved.parents:
        raise ValueError(f"Refusing to remove path outside configured root: {resolved}")
    if path.is_dir():
        shutil.rmtree(path)
    else:
        path.unlink(missing_ok=True)


def write_json_atomic(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(
        json.dumps(value, ensure_ascii=False, indent=2, default=str),
        encoding="utf-8",
    )
    for attempt in range(100):
        try:
            os.replace(temporary, path)
            return
        except PermissionError:
            if attempt == 99:
                raise
            # Windows denies replace while the progress process has the JSON open.
            time.sleep(0.01)


def read_json(path: Path, default: Any) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default


def unlink_with_retry(path: Path) -> None:
    for attempt in range(100):
        try:
            path.unlink(missing_ok=True)
            return
        except PermissionError:
            if attempt == 99:
                raise
            time.sleep(0.01)


def heartbeat(path: Path, phase: str, **values: Any) -> None:
    write_json_atomic(
        path,
        {"phase": phase, "updated_monotonic": time.monotonic(), **values},
    )


@dataclass(frozen=True)
class StageTask:
    key: str
    signature: str
    source_bytes: int
    outputs: tuple[Path, ...]
    call: IsolatedCall
    heartbeat_path: Path


def run_stage_tasks(
    config: AppConfig,
    store: CheckpointStore,
    stage: str,
    tasks: Iterable[StageTask],
    complete: Callable[[StageTask, Any, int, float], tuple[int, int]],
) -> None:
    all_tasks = list(tasks)
    pending = [
        task
        for task in all_tasks
        if not store.is_complete(stage, task.key, task.signature, task.outputs)
    ]
    completed = [task for task in all_tasks if task not in pending]
    progress = StageProgress(
        stage,
        total_bytes=sum(task.source_bytes for task in all_tasks),
        total_tasks=len(all_tasks),
        completed_bytes=sum(task.source_bytes for task in completed),
        completed_tasks=len(completed),
    )
    bar = tqdm(
        total=progress.total_bytes or progress.total_tasks,
        initial=progress.completed_bytes or progress.completed_tasks,
        desc=stage,
        unit="B" if progress.total_bytes else "task",
        unit_scale=bool(progress.total_bytes),
        dynamic_ncols=True,
        disable=not config.runtime.enable_progress,
        bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}, {postfix}]",
    )
    by_key = {task.key: task for task in pending}

    def before_start(call, _active) -> None:
        task = by_key[call.key]
        unlink_with_retry(task.heartbeat_path)
        progress.start(task.key, task.source_bytes)

    def poll(active) -> None:
        for key in active:
            state = read_json(by_key[key].heartbeat_path, {})
            progress.phase(key, str(state.get("phase", "starting")))
        bar.set_postfix_str(progress.render().split("active=", 1)[1])
        bar.refresh()

    def native_retry(call: IsolatedCall, exit_code: int) -> None:
        description = call.fallback_description or "safe execution profile"
        bar.write(
            f"{stage}: task {call.key} exited natively with "
            f"0x{exit_code & 0xFFFFFFFF:08X}; retrying with {description}"
        )

    try:
        for result in run_isolated_many(
            [task.call for task in pending],
            max_workers=max(1, len(config.storage.track_roots)),
            poll_interval_seconds=config.runtime.progress_interval_seconds,
            on_poll=poll,
            before_start=before_start,
            on_native_retry=native_retry,
        ):
            task = by_key[result.key]
            output_bytes, row_count = complete(
                task, result.value, result.process_id, result.elapsed_seconds
            )
            store.complete(
                stage=stage,
                task_key=task.key,
                signature=task.signature,
                source_bytes=task.source_bytes,
                output_paths=task.outputs,
                output_bytes=output_bytes,
                row_count=row_count,
                elapsed_seconds=result.elapsed_seconds,
            )
            unlink_with_retry(task.heartbeat_path)
            progress.complete(task.key, task.source_bytes, result.elapsed_seconds)
            bar.update(task.source_bytes if progress.total_bytes else 1)
            bar.set_postfix_str(progress.render().split("active=", 1)[1])
    finally:
        bar.close()
