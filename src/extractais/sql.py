from __future__ import annotations

from pathlib import Path
from typing import Iterable

from extractais.database import sql_literal


def parquet_sources(paths: Iterable[Path]) -> str:
    values = ", ".join(sql_literal(str(path.resolve())) for path in paths)
    if not values:
        raise ValueError("At least one Parquet source is required")
    return f"read_parquet([{values}], union_by_name = true)"


def haversine_km(lat1: str, lon1: str, lat2: str, lon2: str) -> str:
    return f"""
    6371.0088 * 2 * asin(sqrt(
        least(1.0,
            pow(sin(radians(({lat2}) - ({lat1})) / 2), 2)
            + cos(radians({lat1})) * cos(radians({lat2}))
            * pow(sin(radians(({lon2}) - ({lon1})) / 2), 2)
        )
    ))
    """
