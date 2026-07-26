from __future__ import annotations

import multiprocessing
import os
import traceback
from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class IsolatedResult:
    value: Any
    process_id: int


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


def run_isolated(target: Callable, *args, **kwargs) -> IsolatedResult:
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=_worker_entry,
        args=(sender, target, args, kwargs),
        name=f"extractais-{target.__name__}",
    )
    process.start()
    sender.close()
    payload = None
    try:
        try:
            payload = receiver.recv()
        except EOFError:
            pass
        process.join()
    except BaseException:
        if process.is_alive():
            process.terminate()
        process.join()
        raise
    finally:
        receiver.close()

    if payload is None:
        raise RuntimeError(
            f"Worker {process.pid} exited with code {process.exitcode} without a result"
        )
    if not payload["ok"]:
        raise RuntimeError(
            f"Worker {payload['process_id']} failed: {payload['error']}\n"
            f"{payload['traceback']}"
        )
    if process.exitcode != 0:
        raise RuntimeError(
            f"Worker {payload['process_id']} exited with code {process.exitcode}"
        )
    return IsolatedResult(
        value=payload["value"], process_id=int(payload["process_id"])
    )
