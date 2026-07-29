from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path
from typing import Iterable


class CheckpointStore:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.path = path
        self._lock = threading.Lock()
        self._connection = sqlite3.connect(path, timeout=30, check_same_thread=False)
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA synchronous=FULL")
        self._connection.execute(
            """
            CREATE TABLE IF NOT EXISTS tasks (
                stage TEXT NOT NULL,
                task_key TEXT NOT NULL,
                signature TEXT NOT NULL,
                status TEXT NOT NULL,
                source_bytes INTEGER NOT NULL,
                output_paths TEXT NOT NULL,
                output_bytes INTEGER NOT NULL,
                row_count INTEGER NOT NULL,
                elapsed_seconds REAL NOT NULL,
                completed_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (stage, task_key)
            )
            """
        )
        self._connection.commit()

    def complete(
        self,
        *,
        stage: str,
        task_key: str,
        signature: str,
        source_bytes: int,
        output_paths: Iterable[Path],
        output_bytes: int,
        row_count: int,
        elapsed_seconds: float,
    ) -> None:
        paths = [str(path.resolve()) for path in output_paths]
        with self._lock, self._connection:
            self._connection.execute(
                """
                INSERT INTO tasks (
                    stage, task_key, signature, status, source_bytes,
                    output_paths, output_bytes, row_count, elapsed_seconds
                ) VALUES (?, ?, ?, 'complete', ?, ?, ?, ?, ?)
                ON CONFLICT(stage, task_key) DO UPDATE SET
                    signature=excluded.signature,
                    status='complete',
                    source_bytes=excluded.source_bytes,
                    output_paths=excluded.output_paths,
                    output_bytes=excluded.output_bytes,
                    row_count=excluded.row_count,
                    elapsed_seconds=excluded.elapsed_seconds,
                    completed_at=CURRENT_TIMESTAMP
                """,
                (
                    stage,
                    task_key,
                    signature,
                    int(source_bytes),
                    json.dumps(paths),
                    int(output_bytes),
                    int(row_count),
                    float(elapsed_seconds),
                ),
            )

    def is_complete(
        self,
        stage: str,
        task_key: str,
        signature: str,
        output_paths: Iterable[Path],
    ) -> bool:
        row = self._connection.execute(
            "SELECT signature, status, output_paths FROM tasks WHERE stage=? AND task_key=?",
            (stage, task_key),
        ).fetchone()
        expected = [str(path.resolve()) for path in output_paths]
        return bool(
            row
            and row[0] == signature
            and row[1] == "complete"
            and json.loads(row[2]) == expected
            and all(Path(path).is_file() for path in expected)
        )

    def completed(self, stage: str) -> list[dict[str, object]]:
        rows = self._connection.execute(
            """
            SELECT task_key, source_bytes, output_bytes, row_count, elapsed_seconds
            FROM tasks WHERE stage=? AND status='complete' ORDER BY task_key
            """,
            (stage,),
        ).fetchall()
        return [
            {
                "task_key": row[0],
                "source_bytes": row[1],
                "output_bytes": row[2],
                "row_count": row[3],
                "elapsed_seconds": row[4],
            }
            for row in rows
        ]

    def invalidate(self, stages: Iterable[str]) -> None:
        names = tuple(stages)
        if not names:
            return
        placeholders = ",".join("?" for _ in names)
        with self._lock, self._connection:
            self._connection.execute(
                f"DELETE FROM tasks WHERE stage IN ({placeholders})", names
            )

    def close(self) -> None:
        self._connection.close()

    def __enter__(self) -> "CheckpointStore":
        return self

    def __exit__(self, *_args) -> None:
        self.close()

