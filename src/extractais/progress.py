from __future__ import annotations

import time
from dataclasses import dataclass, field


def format_duration(seconds: float) -> str:
    seconds = max(0, int(seconds))
    hours, remainder = divmod(seconds, 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


@dataclass
class StageProgress:
    name: str
    total_bytes: int
    total_tasks: int
    completed_bytes: int = 0
    completed_tasks: int = 0
    started_at: float = field(default_factory=time.monotonic)
    active: dict[str, tuple[str, float]] = field(default_factory=dict)
    active_bytes: dict[str, int] = field(default_factory=dict)
    samples: list[tuple[int, float]] = field(default_factory=list)

    @property
    def percent(self) -> float:
        if self.total_bytes > 0:
            return min(100.0, self.completed_bytes * 100.0 / self.total_bytes)
        if self.total_tasks > 0:
            return min(100.0, self.completed_tasks * 100.0 / self.total_tasks)
        return 100.0

    def start(self, task_key: str, source_bytes: int, phase: str = "starting") -> None:
        self.active[task_key] = (phase, time.monotonic())
        self.active_bytes[task_key] = max(0, int(source_bytes))

    def phase(self, task_key: str, phase: str) -> None:
        started = self.active.get(task_key, (phase, time.monotonic()))[1]
        self.active[task_key] = (phase, started)

    def complete(self, task_key: str, source_bytes: int, elapsed_seconds: float) -> None:
        self.active.pop(task_key, None)
        self.active_bytes.pop(task_key, None)
        self.completed_bytes += max(0, int(source_bytes))
        self.completed_tasks += 1
        if source_bytes > 0 and elapsed_seconds > 0:
            self.samples.append((int(source_bytes), float(elapsed_seconds)))

    def display_value(self, active_fractions: dict[str, float]) -> int:
        """Include heartbeat estimates without changing committed progress state."""
        if self.total_bytes > 0:
            active_bytes = sum(
                max(0.0, min(1.0, float(fraction)))
                * self.active_bytes.get(key, 0)
                for key, fraction in active_fractions.items()
                if key in self.active
            )
            return min(self.total_bytes, int(self.completed_bytes + active_bytes))
        active_tasks = sum(
            max(0.0, min(1.0, float(fraction)))
            for key, fraction in active_fractions.items()
            if key in self.active
        )
        return min(self.total_tasks, int(self.completed_tasks + active_tasks))

    def _eta(self) -> str:
        if len(self.samples) < 3:
            return f"ETA calibrating {len(self.samples)}/3"
        bytes_done = sum(item[0] for item in self.samples[-32:])
        elapsed = sum(item[1] for item in self.samples[-32:])
        remaining = max(0, self.total_bytes - self.completed_bytes)
        if bytes_done <= 0:
            return "ETA unavailable"
        return f"ETA {format_duration(remaining * elapsed / bytes_done)}"

    def render(self) -> str:
        active = ",".join(
            f"{key}:{phase}" for key, (phase, _) in sorted(self.active.items())
        ) or "-"
        return (
            f"{self.name} {self.percent:6.2f}% "
            f"{self.completed_tasks}/{self.total_tasks} "
            f"active={active} elapsed={format_duration(time.monotonic() - self.started_at)} "
            f"{self._eta()}"
        )
