from __future__ import annotations

import multiprocessing
import os
import time
import traceback
from dataclasses import dataclass
from multiprocessing.connection import wait
from typing import Any, Callable, Iterable, Iterator, Mapping


@dataclass(frozen=True)
class IsolatedResult:
    value: Any
    process_id: int


@dataclass(frozen=True)
class IsolatedCall:
    key: str
    target: Callable
    args: tuple = ()
    kwargs: dict | None = None


@dataclass(frozen=True)
class IsolatedTaskResult:
    key: str
    value: Any
    process_id: int
    elapsed_seconds: float


@dataclass(frozen=True)
class ActiveProcess:
    key: str
    process_id: int
    started_at: float
    phase: str = "running"


def _worker_entry(sender, target: Callable, args: tuple, kwargs: dict) -> None:
    try:
        value = target(*args, **kwargs)
    except BaseException as exc:
        sender.send(
            {
                "ok": False,
                "process_id": os.getpid(),
                "error": f"{type(exc).__name__}: {exc}",
                "traceback": traceback.format_exc(),
            }
        )
        sender.close()
        raise
    sender.send({"ok": True, "process_id": os.getpid(), "value": value})
    sender.close()


def run_isolated(
    target: Callable,
    *args,
    _on_poll: Callable[[Mapping[str, ActiveProcess]], None] | None = None,
    **kwargs,
) -> IsolatedResult:
    result = next(
        run_isolated_many(
            [IsolatedCall(key="single", target=target, args=args, kwargs=kwargs)],
            max_workers=1,
            on_poll=_on_poll,
        )
    )
    return IsolatedResult(value=result.value, process_id=result.process_id)


def run_isolated_many(
    calls: Iterable[IsolatedCall],
    max_workers: int,
    poll_interval_seconds: float = 1.0,
    worker_exit_timeout_seconds: float = 5.0,
    on_poll: Callable[[Mapping[str, ActiveProcess]], None] | None = None,
    before_start: Callable[
        [IsolatedCall, Mapping[str, ActiveProcess]], None
    ]
    | None = None,
) -> Iterator[IsolatedTaskResult]:
    if max_workers <= 0:
        raise ValueError("max_workers must be positive")
    if poll_interval_seconds <= 0:
        raise ValueError("poll_interval_seconds must be positive")
    if worker_exit_timeout_seconds <= 0:
        raise ValueError("worker_exit_timeout_seconds must be positive")

    context = multiprocessing.get_context("spawn")
    pending = iter(calls)
    exhausted = False
    active: dict[str, dict[str, Any]] = {}
    failures: list[dict[str, Any]] = []

    def stop_process(process) -> None:
        process.join(timeout=worker_exit_timeout_seconds)
        if not process.is_alive():
            return
        process.terminate()
        process.join(timeout=worker_exit_timeout_seconds)
        if process.is_alive():
            process.kill()
            process.join()

    def terminate_active() -> None:
        for state in active.values():
            process = state["process"]
            if process.is_alive():
                process.terminate()
        for state in active.values():
            stop_process(state["process"])
            state["receiver"].close()
        active.clear()

    try:
        while not exhausted or active:
            while not exhausted and len(active) < max_workers:
                try:
                    call = next(pending)
                except StopIteration:
                    exhausted = True
                    break
                if call.key in active:
                    raise ValueError(f"Duplicate active isolated-call key: {call.key}")
                if before_start is not None:
                    before_start(
                        call,
                        {
                            key: ActiveProcess(
                                key=key,
                                process_id=int(state["process"].pid),
                                started_at=float(state["started"]),
                                phase=str(state["phase"]),
                            )
                            for key, state in active.items()
                        },
                    )
                receiver, sender = context.Pipe(duplex=False)
                process = context.Process(
                    target=_worker_entry,
                    args=(
                        sender,
                        call.target,
                        call.args,
                        call.kwargs or {},
                    ),
                    name=f"extractais-{call.target.__name__}-{call.key}",
                )
                started = time.perf_counter()
                process.start()
                sender.close()
                active[call.key] = {
                    "receiver": receiver,
                    "process": process,
                    "started": started,
                    "phase": "running",
                }

            if on_poll is not None:
                on_poll(
                    {
                        key: ActiveProcess(
                            key=key,
                            process_id=int(state["process"].pid),
                            started_at=float(state["started"]),
                            phase=str(state["phase"]),
                        )
                        for key, state in active.items()
                    }
                )
            if not active:
                continue

            ready = wait(
                [state["receiver"] for state in active.values()],
                timeout=poll_interval_seconds,
            )
            if not ready:
                continue

            ready_set = set(ready)
            completed_keys = [
                key
                for key, state in active.items()
                if state["receiver"] in ready_set
            ]
            for key in completed_keys:
                state = active[key]
                receiver = state["receiver"]
                process = state["process"]
                payload = None
                try:
                    try:
                        payload = receiver.recv()
                    except EOFError:
                        pass
                    state["phase"] = "worker shutdown"
                    if on_poll is not None:
                        on_poll(
                            {
                                active_key: ActiveProcess(
                                    key=active_key,
                                    process_id=int(active_state["process"].pid),
                                    started_at=float(active_state["started"]),
                                    phase=str(active_state["phase"]),
                                )
                                for active_key, active_state in active.items()
                            }
                        )
                    stop_process(process)
                finally:
                    try:
                        if process.is_alive():
                            stop_process(process)
                    finally:
                        receiver.close()
                        active.pop(key, None)

                if payload is None:
                    failures.append(
                        {
                            "process_id": process.pid,
                            "error": (
                                f"Worker exited with code {process.exitcode} "
                                "without a result"
                            ),
                            "traceback": "",
                        }
                    )
                    exhausted = True
                    continue
                if not payload["ok"]:
                    failures.append(payload)
                    exhausted = True
                    continue
                yield IsolatedTaskResult(
                    key=key,
                    value=payload["value"],
                    process_id=int(payload["process_id"]),
                    elapsed_seconds=time.perf_counter() - float(state["started"]),
                )
        if failures:
            failure = failures[0]
            raise RuntimeError(
                f"Worker {failure['process_id']} failed: "
                f"{failure['error']}\n{failure['traceback']}"
            )
    except BaseException:
        terminate_active()
        raise
