# ExtractAIS

ExtractAIS converts multi-terabyte daily AIS CSV archives into auditable,
time-ordered vessel state intervals. The pipeline is designed for a single
high-memory workstation and uses DuckDB for vectorized, out-of-core execution.

The first stage always separates dynamic position messages from static vessel
messages. Raw CSV files are treated as read-only. Long-running commands show
progress and write restartable manifests.

## Current implementation

- Input inventory and continuity checks
- Dynamic/static AIS separation into compressed Parquet
- Semantic data-quality counters and CSV reject counters
- Atomic outputs and resumable per-file checkpoints
- Aggregate and per-query progress indicators

The port-anchor, port-call, voyage and state-interval stages are specified in
[`docs/pipeline.md`](docs/pipeline.md) and will consume the split Parquet data.

## Environment

```powershell
conda env create -f environment.yml
conda activate extractais
python -m pip install -e .
```

## Production commands

Review `configs/production.example.yaml`, especially `work_root`, before the
first run.

```powershell
extractais --config configs/production.example.yaml inventory
extractais --config configs/production.example.yaml split
```

Use `--limit-files 2` for a small end-to-end check. Use `--force` only when an
existing successful split must be rebuilt.

## Repository policy

CSV data, Parquet outputs, manifests, rejects, temporary files and logs are not
tracked by Git. Every production artifact records its configuration hash and
the Git commit used to create it.
