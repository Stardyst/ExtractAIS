# ExtractAIS pipeline contract

## Execution model

The production archive contains daily CSV files under `2021/` and `2022/`, approximately 6.24 TB in total. DuckDB performs every row-level transformation and may spill to the configured work disk. Python only inventories files, divides work into bounded units and commits checkpoints.

Every heavy work unit runs in a fresh spawned operating-system process and follows the same transaction pattern:

1. Compare the input identity, stage-specific configuration hash and required output files with the manifest.
2. Write to a sibling `.tmp` file or directory.
3. Close DuckDB, return only compact statistics, and exit the worker process.
4. Atomically replace the final output.
5. Record the Git commit, worker PID, row counts where practical, elapsed time and completion timestamp.

An interrupted active unit restarts; completed units are skipped. Process exit is the resource boundary: it releases DuckDB native allocator state, caches, threads and handles that closing a connection alone may retain in a long-lived Python process. Only one heavy worker runs at a time. Stage 01 uses one worker per source day; later boundaries are one month, static compaction, one MMSI bucket, the port catalog, annual export, or the validation report as appropriate.

Before every incomplete work unit, the storage guard requires free bytes to cover both the configured minimum reserve and a conservative estimate of the active unit's output. DuckDB's temporary-directory limit is set from the remaining budget, so external sorts cannot consume the reserved output and safety space. Manifests record free bytes before and after each newly completed unit.

## Data stages

### Stage 01: split

Each CSV is parsed once with all raw columns initially treated as text. Sentinels are normalized, and rows are separated into dynamic message types 1/2/3/18/19/27 and static types 5/24. Dynamic validity requires timestamp, nine-digit MMSI and valid coordinates. Static validity requires timestamp and MMSI. The normalized daily table has one summary scan followed by two Parquet `COPY` operations; each `COPY` result supplies its valid-row count. The manifest records output bytes, compression ratio and free space after every completed day.

Source `matchedPortName`, `label`, `sublabel`, `at_dock`, source and collection type remain evidence only. They never directly determine the computed state.

### Stage 02 and 03: prepare tracks

Static messages are reduced to one row per MMSI using the latest non-null value of each field, with first/last timestamps and message count retained.

Dynamic Parquet is scanned once per month and physically partitioned by `mmsi % bucket_count`. The number of concurrently open partition files is capped independently from the bucket count, preventing hundreds of Parquet writers and row-group buffers from remaining active together on the HDD. Each resulting bucket is then read across all months, exactly deduplicated, sorted by MMSI/time and assigned a per-vessel point sequence. Same-time coordinate conflicts and implied speeds above the configured limit are flagged. Partition and track manifests retain compressed input/output bytes, output rows, effective MiB/s and storage-budget observations.

### Stage 04: stop events

A stop candidate is a low-speed point or a point with missing speed and a sufficiently short step. Consecutive candidates are split on the configured point gap. A run is retained only if its duration and bounding-box diameter satisfy the stop thresholds. The event retains its centroid, extent, speed statistics, point count and source `at_dock` support.

### Stage 05: ports and anchors

WPI rows with invalid coordinates or an excluded Harbor Size are removed. Port centers separated by no more than `group_distance_km` are unioned through connected components. Every member remains in `port_catalog`; recognition outputs the shared `port_group_id`.

Stop centroids are aggregated on the anchor grid. Cells must meet minimum stop-event and distinct-vessel support. Each qualified cell is assigned to the nearest WPI member within `anchor_assignment_radius_km`. The assigned cells become multi-circle anchor centers. Stops within the entry radius of an anchor form independent port-call evidence.

A 0.1-degree lookup table maps AIS `geo_tile` values to possible anchors. Exact Haversine distance is always applied after the coarse join.

### Stage 06: candidates and port calls

For each near-port point, the minimum exact distance to every candidate port group is retained and ranked. Consecutive rank-one points for the same group form an approach episode; missing points, group changes and excessive point gaps split episodes.

An episode becomes a confirmed port call only when it:

- has at least the configured number of points;
- reaches the entry radius and has an exit-radius observation; and
- overlaps an independently detected and port-matched stop event.

The candidate and port-call files preserve source labels, distance margins and evidence counts for audit. Source `at_dock` and port labels do not confirm a call, so they remain independent comparison evidence.

### Stage 07: states and annual intervals

Each valid track point is attached to its most recent and next confirmed port call using temporal ASOF joins. Classification precedence is:

1. Between a confirmed call's entry and exit: `IN_PORT`.
2. Within the approach radius of the previous call: `DEPARTING`.
3. Within the approach radius of the next call: `ARRIVING`.
4. Otherwise: `OCEAN`.

If previous and next approach zones both contain the point, the nearer group determines the direction. Consecutive equal state/from/to points are compressed. Gaps above the configured threshold are emitted separately as `UNKNOWN_GAP`. Segments crossing January 1 are clipped into separate annual rows.

The output intentionally excludes endpoint coordinates, duration and route distance.

## Validation contract

The following evidence must remain available before any Harbor Size is excluded:

- port-group membership and member count;
- anchor location, stop-event support, vessel support and WPI-center distance;
- qualified but unmatched stop-anchor candidates;
- every near-port candidate group and exact distance rank;
- first/second candidate margin;
- matched stop count and source-label support for each call;
- port-level call count, calling-vessel count and ambiguity rate;
- Harbor Size-stratified coverage and quality;
- annual state and quality-flag counts.

`Very Small` removal is a catalog-configuration change. It invalidates stages 05 onward but does not invalidate CSV parsing, static compaction, track preparation or stop detection.

## Known first-version boundary

AIS gaps longer than the threshold are never inferred. Short gaps may remain inside an observed event, but the pipeline does not yet probabilistically reconstruct missing positions or visits. This keeps first-version labels auditable and provides a clean base for a later inference stage.
