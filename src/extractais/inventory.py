from __future__ import annotations

import re
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional

from extractais.config import AppConfig


DATE_PATTERN = re.compile(r"(?P<date>\d{4}-\d{2}-\d{2})", re.IGNORECASE)


@dataclass(frozen=True)
class InputFile:
    path: str
    year: int
    date: str
    size_bytes: int
    modified_ns: int

    @property
    def identity(self) -> str:
        return f"{self.path}|{self.size_bytes}|{self.modified_ns}"

    def to_dict(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class Inventory:
    created_at_utc: str
    files: List[InputFile]
    missing_dates: List[str]
    duplicate_dates: List[str]
    total_size_bytes: int

    def to_dict(self) -> dict:
        return {
            "created_at_utc": self.created_at_utc,
            "files": [item.to_dict() for item in self.files],
            "missing_dates": self.missing_dates,
            "duplicate_dates": self.duplicate_dates,
            "total_size_bytes": self.total_size_bytes,
        }


def _input_file(path: Path, expected_year: Optional[int] = None) -> InputFile:
    match = DATE_PATTERN.search(path.name)
    if not match:
        raise ValueError(f"AIS filename does not contain YYYY-MM-DD: {path}")
    parsed = date.fromisoformat(match.group("date"))
    if expected_year is not None and parsed.year != expected_year:
        raise ValueError(f"File year does not match directory year: {path}")
    stat = path.stat()
    return InputFile(
        path=str(path.resolve()),
        year=parsed.year,
        date=parsed.isoformat(),
        size_bytes=stat.st_size,
        modified_ns=stat.st_mtime_ns,
    )


def discover_files(config: AppConfig, explicit_files: Optional[Iterable[Path]] = None) -> Inventory:
    files: List[InputFile] = []
    if explicit_files:
        files = [_input_file(path.resolve()) for path in explicit_files]
    else:
        for year, directory in sorted(config.input.year_directories.items()):
            year_path = config.input.raw_root / directory
            if not year_path.is_dir():
                raise FileNotFoundError(f"AIS year directory does not exist: {year_path}")
            files.extend(
                _input_file(path, expected_year=year)
                for path in year_path.glob(config.input.filename_pattern)
                if path.is_file()
            )

    files.sort(key=lambda item: (item.date, item.path))
    date_counts = {}
    for item in files:
        date_counts[item.date] = date_counts.get(item.date, 0) + 1

    duplicate_dates = sorted(key for key, count in date_counts.items() if count > 1)
    missing_dates: List[str] = []
    for year in sorted({item.year for item in files}):
        observed = {date.fromisoformat(item.date) for item in files if item.year == year}
        cursor = date(year, 1, 1)
        end = date(year, 12, 31)
        while cursor <= end:
            if cursor not in observed:
                missing_dates.append(cursor.isoformat())
            cursor = cursor.fromordinal(cursor.toordinal() + 1)

    return Inventory(
        created_at_utc=datetime.now(timezone.utc).isoformat(),
        files=files,
        missing_dates=missing_dates,
        duplicate_dates=duplicate_dates,
        total_size_bytes=sum(item.size_bytes for item in files),
    )
