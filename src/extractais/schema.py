from __future__ import annotations

import csv
from pathlib import Path


DYNAMIC_TYPES_DEFAULT = (1, 2, 3, 18, 19, 27)
STATIC_TYPES_DEFAULT = (5, 24)


def sql_int_list(values: list[int]) -> str:
    if not values:
        raise ValueError("Message type list cannot be empty")
    return ", ".join(str(int(value)) for value in values)


RAW_PROJECTION = """
SELECT
    timestamp,
    MMSI,
    msg_type,
    latitude,
    longitude,
    speed,
    course,
    heading,
    rot,
    IMO,
    flag,
    draught,
    ship_and_cargo_type,
    length,
    width,
    eta,
    status,
    maneuver,
    accuracy,
    to_bow,
    to_stern,
    to_port,
    to_starboard,
    collection_type,
    matchedPortName,
    source,
    msg_id,
    ais_version,
    ship_type,
    geopoint_index_id,
    DateOnly,
    s2id,
    label,
    sublabel,
    iso3,
    at_dock
FROM read_csv(
    {input_path},
    header = true,
    {schema_options}
    ignore_errors = true,
    store_rejects = true,
    rejects_table = 'csv_reject_errors',
    rejects_scan = 'csv_reject_scans',
    rejects_limit = 10000,
    strict_mode = false,
    null_padding = false
)
"""


NORMALIZED_PROJECTION = """
SELECT
    try_strptime(regexp_replace(trim(timestamp), '\\s+UTC$', ''), '%Y-%m-%d %H:%M:%S') AS timestamp_utc,
    CASE WHEN regexp_full_match(trim(MMSI), '[0-9]{9}') THEN try_cast(MMSI AS BIGINT) END AS mmsi,
    try_cast(msg_type AS UTINYINT) AS msg_type,
    try_cast(latitude AS DOUBLE) AS latitude,
    try_cast(longitude AS DOUBLE) AS longitude,
    CASE
        WHEN try_cast(speed AS DOUBLE) = 102.3 THEN NULL
        WHEN try_cast(speed AS DOUBLE) BETWEEN 0 AND 102.2 THEN try_cast(speed AS REAL)
    END AS speed,
    CASE
        WHEN try_cast(course AS DOUBLE) >= 0 AND try_cast(course AS DOUBLE) < 360 THEN try_cast(course AS REAL)
    END AS course,
    CASE
        WHEN try_cast(heading AS INTEGER) BETWEEN 0 AND 359 THEN try_cast(heading AS USMALLINT)
    END AS heading,
    try_cast(rot AS REAL) AS rot,
    try_cast(IMO AS BIGINT) AS imo,
    nullif(trim(flag), '') AS flag,
    try_cast(draught AS REAL) AS draught,
    try_cast(ship_and_cargo_type AS INTEGER) AS ship_and_cargo_type,
    try_cast(length AS REAL) AS length,
    try_cast(width AS REAL) AS width,
    nullif(trim(eta), '') AS eta_raw,
    try_cast(status AS SMALLINT) AS navigation_status,
    try_cast(maneuver AS SMALLINT) AS maneuver,
    nullif(trim(accuracy), '') AS accuracy_raw,
    try_cast(to_bow AS REAL) AS to_bow,
    try_cast(to_stern AS REAL) AS to_stern,
    try_cast(to_port AS REAL) AS to_port,
    try_cast(to_starboard AS REAL) AS to_starboard,
    lower(nullif(trim(collection_type), '')) AS collection_type,
    nullif(trim(matchedPortName), '') AS matched_port_name,
    nullif(trim(source), '') AS source,
    nullif(trim(msg_id), '') AS msg_id,
    try_cast(ais_version AS SMALLINT) AS ais_version,
    try_cast(ship_type AS INTEGER) AS ship_type,
    regexp_extract(geopoint_index_id, '[0-9a-fA-F]{15}', 0) AS h3_r3,
    nullif(trim(s2id), '') AS s2id,
    nullif(trim(label), '') AS source_label,
    nullif(trim(sublabel), '') AS source_sublabel,
    nullif(trim(iso3), '') AS iso3,
    try_cast(at_dock AS BOOLEAN) AS source_at_dock
FROM raw_day
"""


DYNAMIC_SELECT = """
SELECT
    timestamp_utc,
    mmsi,
    msg_type,
    latitude,
    longitude,
    speed,
    course,
    heading,
    rot,
    navigation_status,
    maneuver,
    accuracy_raw,
    collection_type,
    matched_port_name,
    source,
    msg_id,
    ship_type,
    h3_r3,
    s2id,
    source_label,
    source_sublabel,
    iso3,
    source_at_dock,
    cast(floor((latitude + 90.0) * 10.0) AS INTEGER) * 3601
        + cast(floor((longitude + 180.0) * 10.0) AS INTEGER) AS geo_tile
FROM normalized_day
WHERE msg_type IN ({dynamic_types})
  AND timestamp_utc IS NOT NULL
  AND mmsi IS NOT NULL
  AND latitude BETWEEN -90 AND 90
  AND longitude BETWEEN -180 AND 180
"""


STATIC_SELECT = """
SELECT
    timestamp_utc,
    mmsi,
    msg_type,
    imo,
    flag,
    draught,
    ship_and_cargo_type,
    length,
    width,
    eta_raw,
    to_bow,
    to_stern,
    to_port,
    to_starboard,
    ais_version,
    ship_type,
    collection_type,
    source,
    msg_id
FROM normalized_day
WHERE msg_type IN ({static_types})
  AND timestamp_utc IS NOT NULL
  AND mmsi IS NOT NULL
"""


def _safe_raw_projection(input_path: Path) -> str:
    from extractais.database import sql_literal

    with input_path.open("r", encoding="utf-8-sig", newline="") as handle:
        try:
            header = next(csv.reader(handle))
        except StopIteration as exc:
            raise ValueError(f"CSV is empty: {input_path}") from exc
    if not header or any(not name.strip() for name in header):
        raise ValueError(f"CSV has an empty header name: {input_path}")
    normalized = [name.strip().casefold() for name in header]
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"CSV has duplicate header names: {input_path}")
    required = {
        "timestamp", "mmsi", "msg_type", "latitude", "longitude", "speed",
        "course", "heading", "rot", "imo", "flag", "draught",
        "ship_and_cargo_type", "length", "width", "eta", "status",
        "maneuver", "accuracy", "to_bow", "to_stern", "to_port",
        "to_starboard", "collection_type", "matchedportname", "source",
        "msg_id", "ais_version", "ship_type", "geopoint_index_id",
        "dateonly", "s2id", "label", "sublabel", "iso3", "at_dock",
    }
    missing = sorted(required - set(normalized))
    if missing:
        raise ValueError(f"CSV is missing required columns {missing}: {input_path}")
    schema = ", ".join(
        f"{sql_literal(name.strip())}: 'VARCHAR'" for name in header
    )
    return RAW_PROJECTION.format(
        input_path=sql_literal(str(input_path.resolve())),
        schema_options=(
            f"columns = {{{schema}}},\n"
            "    auto_detect = false,\n"
            "    parallel = false,"
        ),
    )


def normalized_day_sql(input_path: Path, *, safe_mode: bool = False) -> str:
    from extractais.database import sql_literal

    raw = (
        _safe_raw_projection(input_path)
        if safe_mode
        else RAW_PROJECTION.format(
            input_path=sql_literal(str(input_path.resolve())),
            schema_options="all_varchar = true,",
        )
    )
    return f"""
        WITH raw_day AS ({raw})
        {NORMALIZED_PROJECTION}
    """
