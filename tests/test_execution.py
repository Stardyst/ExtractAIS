import multiprocessing
import os
import threading
import time
from pathlib import Path

import pytest

from extractais.isolated import IsolatedCall, run_isolated_many
from extractais.progress import (
    HONEST_BAR_FORMAT,
    FRACTIONAL_BAR_FORMAT,
    ProgressEstimator,
    advance_progress_to,
)
from extractais.storage import parse_size_bytes


def _sleep_and_return(delay: float) -> str:
    time.sleep(delay)
    return "complete"


def _return_with_lingering_thread(delay: float) -> str:
    thread = threading.Thread(target=time.sleep, args=(delay,), daemon=False)
    thread.start()
    return "result-sent"


def _fail_after(delay: float) -> None:
    time.sleep(delay)
    raise RuntimeError("expected worker failure")


def _write_marker_after(path: str, delay: float) -> str:
    time.sleep(delay)
    Path(path).write_text("complete", encoding="utf-8")
    return "sibling-complete"


def test_eta_is_hidden_until_real_work_unit_completes() -> None:
    estimator = ProgressEstimator(max_workers=2)

    assert estimator.format_eta(10_000_000) == "ETA calibrating"

    estimator.add_sample(source_bytes=5_000_000, elapsed_seconds=5)
    assert estimator.format_eta(10_000_000) == "ETA 00:00:05"
    assert estimator.format_eta(10_000_000, worker_count=1) == "ETA 00:00:10"
    assert "remaining" not in HONEST_BAR_FORMAT
    assert "{n_fmt}" not in FRACTIONAL_BAR_FORMAT
    assert "{percentage:6.2f}" in FRACTIONAL_BAR_FORMAT


def test_fractional_progress_advances_monotonically_before_completion() -> None:
    class ProgressRecorder:
        def __init__(self) -> None:
            self.n = 0.0
            self.total = 2.0
            self.values: list[float] = []

        def refresh(self) -> None:
            self.values.append(self.n)

    progress = ProgressRecorder()

    advance_progress_to(progress, 0.10)
    advance_progress_to(progress, 0.35)
    advance_progress_to(progress, 0.20)
    advance_progress_to(progress, 1.00)

    assert progress.values == [0.10, 0.35, 1.00]
    assert 0 < progress.values[0] < progress.total


def test_duckdb_memory_limit_parser_matches_decimal_and_binary_units() -> None:
    assert parse_size_bytes("80GB") == 80_000_000_000
    assert parse_size_bytes("64MB") == 64_000_000
    assert parse_size_bytes("1GiB") == 1024**3


def test_isolated_scheduler_never_exceeds_worker_limit() -> None:
    active_counts: list[int] = []
    calls = [
        IsolatedCall(
            key=f"unit-{index}",
            target=_sleep_and_return,
            args=(0.2,),
        )
        for index in range(4)
    ]

    results = list(
        run_isolated_many(
            calls,
            max_workers=2,
            poll_interval_seconds=0.02,
            on_poll=lambda active: active_counts.append(len(active)),
        )
    )

    assert {result.key for result in results} == {
        "unit-0", "unit-1", "unit-2", "unit-3"
    }
    assert all(result.value == "complete" for result in results)
    assert all(result.process_id != os.getpid() for result in results)
    assert max(active_counts) == 2


def test_result_delivery_is_not_blocked_by_worker_shutdown() -> None:
    started = time.perf_counter()

    results = list(
        run_isolated_many(
            [
                IsolatedCall(
                    key="lingering",
                    target=_return_with_lingering_thread,
                    args=(2.0,),
                )
            ],
            max_workers=1,
            poll_interval_seconds=0.02,
            worker_exit_timeout_seconds=0.1,
        )
    )

    assert results[0].value == "result-sent"
    assert time.perf_counter() - started < 1.0


def test_worker_failure_drains_active_siblings_before_raising(
    tmp_path: Path,
) -> None:
    marker = tmp_path / "sibling-complete.txt"
    results = []

    with pytest.raises(RuntimeError, match="expected worker failure"):
        for result in run_isolated_many(
            [
                IsolatedCall(key="failure", target=_fail_after, args=(0.05,)),
                IsolatedCall(
                    key="sibling",
                    target=_write_marker_after,
                    args=(str(marker), 0.2),
                ),
                IsolatedCall(key="not-started", target=_sleep_and_return, args=(0.01,)),
            ],
            max_workers=2,
            poll_interval_seconds=0.02,
        ):
            results.append(result)

    assert marker.read_text(encoding="utf-8") == "complete"
    assert [result.key for result in results] == ["sibling"]


def test_poll_failure_during_worker_shutdown_does_not_orphan_process() -> None:
    shutdown_pids: list[int] = []

    def fail_during_shutdown(active) -> None:
        for process in active.values():
            if process.phase == "worker shutdown":
                shutdown_pids.append(process.process_id)
                raise RuntimeError("poll callback failed")

    with pytest.raises(RuntimeError, match="poll callback failed"):
        list(
            run_isolated_many(
                [
                    IsolatedCall(
                        key="lingering",
                        target=_return_with_lingering_thread,
                        args=(2.0,),
                    )
                ],
                max_workers=1,
                poll_interval_seconds=0.02,
                worker_exit_timeout_seconds=0.1,
                on_poll=fail_during_shutdown,
            )
        )

    active_child_pids = {process.pid for process in multiprocessing.active_children()}
    assert shutdown_pids
    assert not active_child_pids.intersection(shutdown_pids)
