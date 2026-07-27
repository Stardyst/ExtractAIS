import os
import time

from extractais.isolated import IsolatedCall, run_isolated_many
from extractais.progress import HONEST_BAR_FORMAT, ProgressEstimator


def _sleep_and_return(delay: float) -> str:
    time.sleep(delay)
    return "complete"


def test_eta_is_hidden_until_real_work_unit_completes() -> None:
    estimator = ProgressEstimator(max_workers=2)

    assert estimator.format_eta(10_000_000) == "ETA calibrating"

    estimator.add_sample(source_bytes=5_000_000, elapsed_seconds=5)
    assert estimator.format_eta(10_000_000) == "ETA 00:00:05"
    assert estimator.format_eta(10_000_000, worker_count=1) == "ETA 00:00:10"
    assert "remaining" not in HONEST_BAR_FORMAT


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
