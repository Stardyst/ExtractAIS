from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Iterable

from tqdm import tqdm

from extractais.config import AppConfig
from extractais.isolated import IsolatedCall, IsolatedTaskResult, run_isolated_many
from extractais.progress import HONEST_BAR_FORMAT, ProgressEstimator
from extractais.storage import TIB, ensure_parallel_storage_budget, free_space_bytes


@dataclass(frozen=True)
class BucketTask:
    key: str
    label: str
    source_bytes: int
    estimated_output_bytes: int
    call: IsolatedCall


@dataclass(frozen=True)
class BucketExecution:
    task: BucketTask
    result: IsolatedTaskResult
    free_space_bytes_before: int


def run_bucket_stage(
    config: AppConfig,
    description: str,
    tasks: Iterable[BucketTask],
    total_count: int,
    completed_count: int,
    completed_samples: Iterable[tuple[int, float]],
    on_complete: Callable[[BucketExecution], str],
) -> None:
    pending = list(tasks)
    task_by_key = {task.key: task for task in pending}
    if len(task_by_key) != len(pending):
        raise ValueError(f"Duplicate task key in {description}")

    estimator = ProgressEstimator(max_workers=config.runtime.bucket_workers)
    for source_bytes, elapsed_seconds in completed_samples:
        estimator.add_sample(source_bytes, elapsed_seconds)
    remaining_bytes = sum(task.source_bytes for task in pending)
    remaining_count = len(pending)
    free_before_by_key: dict[str, int] = {}

    progress = tqdm(
        total=total_count,
        initial=completed_count,
        desc=description,
        disable=not config.runtime.enable_progress,
        dynamic_ncols=True,
        bar_format=HONEST_BAR_FORMAT,
    )

    def before_start(call, active) -> None:
        task = task_by_key[call.key]
        active_output_bytes = sum(
            task_by_key[key].estimated_output_bytes for key in active
        )
        free_before_by_key[call.key] = ensure_parallel_storage_budget(
            config,
            f"{description}: {task.label}",
            active_output_bytes,
            task.estimated_output_bytes,
        )

    def on_poll(active) -> None:
        labels = ",".join(task_by_key[key].label for key in sorted(active))
        progress.set_postfix_str(
            f"active={labels or '-'} "
            f"{estimator.format_eta(remaining_bytes, remaining_count)}"
        )
        progress.refresh()

    calls = [task.call for task in pending]
    try:
        for result in run_isolated_many(
            calls,
            max_workers=config.runtime.bucket_workers,
            on_poll=on_poll,
            before_start=before_start,
        ):
            task = task_by_key[result.key]
            execution = BucketExecution(
                task=task,
                result=result,
                free_space_bytes_before=free_before_by_key[result.key],
            )
            detail = on_complete(execution)
            estimator.add_sample(task.source_bytes, result.elapsed_seconds)
            remaining_bytes -= task.source_bytes
            remaining_count -= 1
            progress.update(1)
            free_tib = free_space_bytes(config.storage.work_root) / TIB
            progress.set_postfix_str(
                f"{task.label} {detail} "
                f"{estimator.format_eta(remaining_bytes, remaining_count)} "
                f"free={free_tib:.2f}TiB"
            )
    finally:
        progress.close()
