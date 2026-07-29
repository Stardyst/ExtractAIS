from __future__ import annotations

import statistics
from collections import deque


HONEST_BAR_FORMAT = (
    "{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} "
    "[{elapsed}, {postfix}]"
)
FRACTIONAL_BAR_FORMAT = (
    "{desc}: {percentage:6.2f}%|{bar}| [{elapsed}, {postfix}]"
)


def advance_progress_to(progress, value: float) -> None:
    """Advance a tqdm-compatible progress object without moving backwards."""
    bounded = min(float(progress.total), max(float(progress.n), float(value)))
    if bounded > float(progress.n):
        progress.n = bounded
        progress.refresh()


def format_duration(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    hours, remainder = divmod(total, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


class ProgressEstimator:
    def __init__(self, max_workers: int, sample_window: int = 12) -> None:
        if max_workers <= 0:
            raise ValueError("max_workers must be positive")
        self.max_workers = max_workers
        self._rates: deque[float] = deque(maxlen=sample_window)

    def add_sample(self, source_bytes: int, elapsed_seconds: float) -> None:
        if source_bytes > 0 and elapsed_seconds > 0:
            self._rates.append(source_bytes / elapsed_seconds)

    @property
    def has_samples(self) -> bool:
        return bool(self._rates)

    @property
    def aggregate_bytes_per_second(self) -> float | None:
        if not self._rates:
            return None
        return statistics.median(self._rates) * self.max_workers

    def estimate_seconds(
        self, remaining_bytes: int, worker_count: int | None = None
    ) -> float | None:
        if not self._rates:
            return None
        effective_workers = self.max_workers
        if worker_count is not None:
            effective_workers = max(1, min(self.max_workers, worker_count))
        rate = statistics.median(self._rates) * effective_workers
        return max(0, remaining_bytes) / rate

    def format_eta(
        self, remaining_bytes: int, worker_count: int | None = None
    ) -> str:
        seconds = self.estimate_seconds(remaining_bytes, worker_count)
        if seconds is None:
            return "ETA calibrating"
        return f"ETA {format_duration(seconds)}"
