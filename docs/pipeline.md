# ExtractAIS processing pipeline

## Data scale and execution model

The production archive contains daily files under `2021/` and `2022/`, each
roughly 8 GB. Bulk transformations must use vectorized DuckDB SQL and Parquet;
Python coordinates work units and never iterates over individual AIS rows.

All stages are restartable. A stage writes temporary artifacts first, replaces
the final artifact atomically, then updates its manifest. Aggregate progress is
shown by input bytes, month or MMSI bucket. DuckDB reports progress for the
active query.

## Stages

1. Inventory source files and verify date continuity.
2. Parse each CSV once and immediately separate dynamic and static messages.
3. Repartition dynamic Parquet by MMSI hash and sort each bucket by MMSI/time.
4. Generate stop events and data-supported port anchors.
5. Build port groups and 3/4/10 km multi-anchor geofences.
6. Match near-port points through a coarse spatial tile and exact distance.
7. Confirm port calls, construct voyages and assign the five spatial states.
8. Compress consecutive states to intervals and split only at year boundaries.
9. Produce port-level metrics, ambiguous events and stratified review samples.

## Stage 1 outputs

Dynamic and static records are written separately under:

```text
stage01_split/dynamic/year=YYYY/month=MM/day=DD/part.parquet
stage01_split/static/year=YYYY/month=MM/day=DD/part.parquet
```

The dynamic table contains only message types 1, 2, 3, 18, 19 and 27 with a
valid UTC timestamp, nine-digit MMSI and valid coordinates. AIS unavailable
sentinels are converted to null. Static message types 5 and 24 require a valid
UTC timestamp and MMSI. Other message types are counted but not exported.

Source-provided `matchedPortName`, `label`, `sublabel` and `at_dock` are retained
in the dynamic output solely as validation evidence.

## Disk budget

The 4.48 TB work volume must not be committed to a full run until the two-month
pilot measures dynamic Parquet size and external-sort temporary space. Stage
manifests report output rows and elapsed time per day so the full-run estimate is
based on observed throughput.

## Port validation contract

The port stage must preserve anchor support, the three closest port candidates,
distance margins, entry/exit evidence, source-label agreement and ambiguity
flags. Metrics are stratified by WPI Harbor Size. Removing `Very Small` ports is
a catalog decision after review and must not require reparsing source CSV.
