# ExtractAIS pipeline contract

## Execution model

The production archive contains daily CSV files under `2021/` and `2022/`, approximately 6.24 TB in total. DuckDB performs every row-level transformation and may spill to the configured work disk. Python only inventories files, divides work into bounded units and commits checkpoints.

Every heavy work unit runs in a fresh spawned operating-system process and follows the same transaction pattern:

1. Compare the input identity, stage-specific configuration hash and required output files with the manifest.
2. Write to a sibling `.tmp` file or directory.
3. Close DuckDB, return only compact statistics, and exit the worker process.
4. Atomically replace the final output.
5. Record the Git commit, worker PID, row counts where practical, elapsed time and completion timestamp.

An interrupted active unit restarts; completed units are skipped. Track preparation additionally checkpoints every committed hash shard inside a source bucket, so retry only rebuilds missing shards. Process exit is the resource boundary: it releases DuckDB native allocator state, caches, threads and handles that closing a connection alone may retain in a long-lived Python process. Result delivery has a bounded worker-exit wait, so interpreter shutdown cannot block the parent indefinitely. After the first worker failure, no new unit starts; already active units drain and commit before the original error is raised. Global units use one worker with the global thread/memory profile. Track-work-unit stages use one spawned worker on the production HDD with a bounded thread, memory and task-specific temporary-directory profile.

Before every incomplete work unit, the storage guard requires free bytes to cover the configured minimum reserve and a conservative estimate of the active output. Bucket scheduling also reserves every worker's maximum DuckDB temporary budget before launching another process. Manifests record source/output bytes, effective throughput and free bytes before and after each newly completed unit.

DuckDB's internal operator progress is disabled because it does not account reliably for blocking finalization and Parquet flushes. Parent progress bars derive ETA only from the full elapsed time of completed compatible units. They show `ETA calibrating` before a real sample exists and reduce assumed parallelism when fewer work units remain. Track preparation reports a separate monotonic fraction: source re-sharding advances from output-byte growth, and each shard advances at deduplication, conflict detection, sequencing and commit boundaries. Downstream stages inherit the smaller permanent work units and therefore commit progress frequently.

## Data stages

### Stage 01: split

Each CSV is parsed once with all raw columns initially treated as text. Sentinels are normalized, and rows are separated into dynamic message types 1/2/3/18/19/27 and static types 5/24. Dynamic validity requires timestamp, nine-digit MMSI and valid coordinates. Static validity requires timestamp and MMSI. The normalized daily table has one summary scan followed by two Parquet `COPY` operations; each `COPY` result supplies its valid-row count. The manifest records output bytes, compression ratio and free space after every completed day.

Source `matchedPortName`, `label`, `sublabel`, `at_dock`, source and collection type remain evidence only. They never directly determine the computed state.

### Stage 02 and 03: prepare tracks

Static messages are reduced to one row per MMSI using the latest non-null value of each field, with first/last timestamps and message count retained.

Dynamic Parquet is scanned once per month and physically partitioned by `mmsi % bucket_count`. One writer remains open for every bucket. A lower writer limit causes DuckDB to evict and recreate partition writers, multiplying a production month into thousands of small files; configuration validation therefore requires `partition_write_max_open_files >= mmsi_buckets`, and the completed month is rejected if its file count exceeds the bucket count. Row-group size, rather than writer eviction, bounds buffering.

The 256 monthly partitions are coarse source buckets. A full-archive source bucket can reach 13.33 GiB compressed, which expands beyond an 80 GB DuckDB window-memory limit. Stage 03 therefore scans one source bucket once and partitions it by `hash(mmsi)`. The shard count is a power of two selected from compressed source size and per-thread memory, with a 512 MiB target, a minimum of 8 and a maximum of 64. A 13.33 GiB production bucket becomes 32 shards. Since the hash key is MMSI, a vessel's complete multi-year trajectory remains in exactly one shard.

Each permanent shard is built through three bounded queries with Parquet materialization between them: exact coordinate/time deduplication, same-time coordinate-conflict detection, and vessel sequencing with gap, distance and implied-speed flags. Closing the connection between queries releases blocking window state before the next sort. Every shard is atomically committed to its own `stage03_tracks/mmsi_bucket=NNNNN/part.parquet` file and recorded in a source-bucket checkpoint. Intermediate re-sharding, query outputs and DuckDB spill files exist only in the active source bucket's deterministic temporary directory. Stage 04 onward consumes these permanent shards directly rather than recombining them into large files.

The stage-03 layout marker is versioned independently from the monthly/static preparation hash. Moving from the legacy large-bucket layout removes old stage-03 tracks and every downstream artifact or manifest that depends on the old physical IDs. Completed split outputs, monthly partitions and the compact static table remain valid. This prevents unmatched legacy bucket files from entering a new global port or validation scan. Partition manifests retain compressed input/output bytes, output file count, rows, effective MiB/s and storage-budget observations.

### Stage 04: stop events

A stop candidate is a low-speed point or a point with missing speed and a sufficiently short step. Consecutive candidates are split on the configured point gap. A run is retained only if its duration and bounding-box diameter satisfy the stop thresholds. The event retains its centroid, extent, speed statistics, point count and source `at_dock` support.

### Stage 05: ports and anchors

WPI rows with invalid coordinates or an excluded Harbor Size are removed. Port centers separated by no more than `group_distance_km` are unioned through connected components. Every member remains in `port_catalog`; recognition outputs the shared `port_group_id`.

Stop centroids are aggregated on the anchor grid. Cells must meet minimum stop-event and distinct-vessel support. Each qualified cell is assigned to the nearest WPI member within `anchor_assignment_radius_km`. The assigned cells become multi-circle anchor centers. Stops within the entry radius of an anchor form independent port-call evidence. This stop-to-port matching is written and resumed per permanent track work unit rather than materialized as one global table.

A 0.1-degree lookup table maps AIS `geo_tile` values to possible anchors. Exact Haversine distance is always applied after the coarse join.

### Stage 06: candidates and port calls

For each near-port point, the minimum exact distance to every candidate port group is retained and ranked. A compact one-row-per-point context stores the first and second candidate, both distances and the ambiguity margin so downstream queries do not repeat the rank-one/rank-two self-join. Consecutive rank-one points for the same group form an approach episode; missing points, group changes and excessive point gaps split episodes.

An episode becomes a confirmed port call only when it:

- has at least the configured number of points;
- reaches the entry radius and has an exit-radius observation; and
- overlaps an independently detected and port-matched stop event.

The candidate and port-call files preserve source labels, distance margins and evidence counts for audit. Source `at_dock` and port labels do not confirm a call, so they remain independent comparison evidence.

### Final states and annual intervals

Each valid track point is attached to its most recent and next confirmed port call using temporal ASOF joins. Classification precedence is:

1. Between a confirmed call's entry and exit: `IN_PORT`.
2. Within the approach radius of the previous call: `DEPARTING`.
3. Within the approach radius of the next call: `ARRIVING`.
4. Otherwise: `OCEAN`.

If previous and next approach zones both contain the point, the nearer group determines the direction. Consecutive equal state/from/to points are compressed. Gaps above the configured threshold are emitted separately as `UNKNOWN_GAP`. Segments crossing January 1 are clipped into separate annual rows. Each track-work-unit worker writes its annual files directly to `outputs/trajectory_intervals/year=YYYY`; there is no duplicate global annual-export scan or persistent stage-07 copy.

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
