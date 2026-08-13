# ExtractAIS 2.2

ExtractAIS 将按日 CSV 形式保存的超大规模 AIS 档案，转换为按 MMSI 和时间排序的年度船舶运行区间。v2 是不兼容重构：原始 CSV 每日只解析一次，动态信息和静态信息立即分离；船舶全量排序只执行一次；重任务按固定 MMSI 分区分布到独立物理盘。

最终状态为 `IN_PORT`、`DEPARTING`、`OCEAN`、`ARRIVING` 和 `UNKNOWN_GAP`。港口识别采用锚点多圆港区，无法可靠区分的近邻港口输出共同的 `port_group_id`。

## 1. 安装与升级

```powershell
conda env create -f environment.yml
conda activate extractais
python -m pip install -e .
python -c "import duckdb, extractais; print(extractais.__version__, duckdb.__version__)"
```

版本必须是 `ExtractAIS 2.2.0`、`DuckDB 1.5.4`。已有环境升级：

```powershell
git pull origin main
conda activate extractais
python -m pip install --upgrade -e .
python -c "import duckdb, extractais; print(extractais.__version__, duckdb.__version__)"
```

## 2. 从旧版迁移

v2 不读取 v1 的 `derived`、manifest 或中间 Parquet。确认没有旧进程后，旧方案新增内容可通过删除以下目录全部释放：

```powershell
Remove-Item -LiteralPath "E:\AIS2021-2022\derived" -Recurse
```

执行前必须核对该绝对路径。原始 `2021`、`2022`、`UpdatedPub150.csv`、仓库和日志不在删除范围内。程序本身不会自动删除 v1 数据。

## 3. 生产配置与磁盘布局

```powershell
Copy-Item configs/production.example.yaml configs/production.yaml
notepad configs/production.yaml
extractais --config configs/production.yaml inventory
```

示例配置按照主机的物理磁盘关系设置：

| 位置 | 作用 | 生命周期 |
|---|---|---|
| `E:\AIS2021-2022\2021,2022` | 原始 CSV，只读 | 永久 |
| `F:\ExtractAIS-v2` | 偶数轨迹分区 | 永久 |
| `G:\ExtractAIS-v2` | 奇数轨迹分区 | 永久 |
| `D:\ExtractAIS-v2-temp` | DuckDB spill、心跳、当前分区去重/冲突中间文件 | 成功结束后清空 |
| `H:\ExtractAIS-v2\products` | 区间、港口调用、静态船舶、验证和检查点 | 永久 |
| `E:\...\ExtractAIS-v2-evidence`、`I:\ExtractAIS-v2\evidence` | 点到 WPI 港口的最近锚点证据和港口上下文 | 永久但可重建 |

`H` 到 `L` 属于同一物理盘，不能把不同盘符当成额外并行磁盘。`F/G` 各自只运行一个重型任务；每个任务内部使用 6 个 DuckDB 线程和 40 GB 上限。不要将 `workers_per_track_root` 改为 2。

每个根目录都有独立空间保护。启动下一个工作单元前，程序要求：

```text
当前可用空间 >= 配置保留空间 + 当前工作单元预计输出/临时空间
```

不满足时会在写入前停止，已原子提交的数据和 SQLite 检查点保持有效。

## 4. 完整运行

推荐一次执行并保留日志：

```powershell
New-Item -ItemType Directory -Force logs | Out-Null
extractais --config configs/production.yaml run-all 2>&1 |
  Tee-Object -FilePath logs/run-all-v2.log
```

也可逐阶段执行：

```powershell
extractais --config configs/production.yaml ingest
extractais --config configs/production.yaml tracks
extractais --config configs/production.yaml ports
extractais --config configs/production.yaml geometry
extractais --config configs/production.yaml calls
extractais --config configs/production.yaml intervals
extractais --config configs/production.yaml validate
```

`tracks` 会自动补齐缺失的 `ingest` 工作单元，并且只有全部 canonical track 分区完成后才删除 ingest runs。后续命令要求其直接依赖已经存在。最短且不易误操作的入口仍是 `run-all`。

| 阶段 | 工作内容 | 检查点粒度 |
|---|---|---|
| `ingest` | 每个 CSV 解析一次；动态/静态分离；按最终轨迹盘写有序 run | 每日文件 |
| `tracks` | 分阶段完成精确去重、冲突索引、MMSI 排序和异常标记；同时产出静态船舶和停留事件 | 1024 个固定分区 |
| `ports` | WPI 清洗、近邻港口组、停留密度锚点和多圆港区 | 分区锚点单元 + 全局目录 |
| `geometry` | 分片计算每个 AIS 点到各 WPI 港口的最近锚点证据 | 固定分区 |
| `calls` | 将 WPI 港口候选映射到当前港口组并确认港口调用 | 固定分区 |
| `intervals` | 五状态分类、长中断、连续状态压缩、年度裁切 | 固定分区 |
| `validate` | 港口覆盖、歧义、Harbor Size、状态和证据汇总 | 固定分区 + 全局汇总 |

`run-all` 到 `validate` 为止。高成本深度质量审计是独立命令，不会在常规重跑时自动执行；完整流程成功后按第 10 节运行一次即可。

## 5. 进度与恢复

进度条同时显示两种状态：条形百分比按分区心跳中的内部阶段保守推进，SQLite 检查点和 ETA 吞吐样本只认原子提交完成。例如：

```text
tracks: 85%|...| 452G/535G [28:08:05, active=0866:3/5 trajectory sequencing out=0.31GiB free=8.42TiB ETA 10:12:30]
```

- `active=分区:阶段` 每 5 秒从子进程心跳更新，即使百分比暂时不变也能看到任务在做什么。
- `tracks` 的内部阶段固定为 `1/5 exact deduplication`、`2/5 time conflict index`、`3/5 trajectory sequencing`、`4/5 stop event detection` 和 `5/5 vessel static compaction`。
- `geometry` 依次显示锚点边分片、`point-port shard n/N` 压缩、紧凑候选合并和停留事件匹配。前两步的 `out` 是 D 盘当前临时边/紧凑分片大小。
- `out` 是当前阶段已经写出的临时文件大小，`free` 是该阶段目标盘实时可用空间；二者变化可用于区分长查询与停滞。
- ETA 至少完成 3 个工作单元后才显示；此前显示 `ETA calibrating n/3`。
- ETA 基于已提交工作单元的实际字节吞吐，不使用“首个任务瞬间完成”造成的虚假速度。
- 一个分区运行时，条形进度会按 `n/N` 内部阶段移动；中断后仍从该分区开头重做，只有提交后的分区可以跳过。
- 某日并行 CSV reader 若被 Windows 原生层终止，该日会自动使用显式 schema 的单线程 reader 重试一次；已完成日期不会重算，解析规则不变。

可随时 `Ctrl+C` 停止，再执行同一命令。`state.sqlite` 同时验证参数签名和输出文件存在性；未提交的 `.tmp` 不会被当成成功结果。查看状态：

```powershell
extractais --config configs/production.yaml status
extractais --config configs/production.yaml status --json
```

SQLite 检查点位于 `H:\ExtractAIS-v2\products\metadata\state.sqlite`。

### 从 2.0.1 的 tracks 原生崩溃恢复

若日志包含 `0xC0000005` 且活动阶段为 `sorting canonical trajectory`，不要删除任何输出、`ingest_runs` 或 `state.sqlite`。停止旧进程后执行：

```powershell
git pull origin main
conda activate extractais
python -m pip install --upgrade -e .
python -c "import duckdb, extractais; print(extractais.__version__, duckdb.__version__)"
extractais --config configs/production.yaml status
extractais --config configs/production.yaml run-all 2>&1 |
  Tee-Object -FilePath logs/run-all-v2-resume.log
```

应输出 `2.2.0 1.5.4`。2.2.0 不改变 `tracks` 的任务签名和最终 Parquet 契约，因此已提交分区直接跳过；失败分区遗留的 `.tmp.parquet` 和 `D:\ExtractAIS-v2-temp\tracks-NNNN` 会在该分区启动时清理。`ingest` 已有检查点和每日 run 会直接复用。

### 从 2.0.2 的 geometry 候选爆炸恢复

2.0.2 为每个 AIS 点永久保存 10 km 内的全部锚点，并对候选全局排序。生产样本中单个 `0.877 GiB` 分区产生了 `16.28 亿` 条记录、`10.969 GiB` 输出和超过 `180 GiB` DuckDB spill。2.1.0 改为 64 个临时边分片，并在不依赖港口组的前提下只保留每个 WPI 港口的最近锚点。

升级后直接执行：

```powershell
git pull origin main
conda activate extractais
python -m pip install --upgrade -e .
python -c "import duckdb, extractais; print(extractais.__version__, duckdb.__version__)"
extractais --config configs/production.yaml geometry
```

`ingest`、`tracks` 和 `ports` 检查点会完整复用；geometry 的新路径和契约版本会使旧 geometry 检查点失效，随后 `calls`、`intervals`、`validate` 自动重建。确认没有运行中的 ExtractAIS 进程后，2.0.2 遗留的以下目录可以删除：

```powershell
Remove-Item -LiteralPath "E:\AIS2021-2022\ExtractAIS-v2-evidence\point_anchor_candidates" -Recurse
Remove-Item -LiteralPath "I:\ExtractAIS-v2\evidence\point_anchor_candidates" -Recurse
```

执行前必须逐一核对绝对路径。2.1.0 的新证据目录名为 `point_port_candidates`，不在删除范围内。

## 6. 最终区间字段

最终目录：

```text
H:\ExtractAIS-v2\products\trajectory_intervals\year=2021\partition=NNNN.parquet
H:\ExtractAIS-v2\products\trajectory_intervals\year=2022\partition=NNNN.parquet
```

每行是一个连续状态区间，共 24 个字段：

| 字段 | 含义 |
|---|---|
| `year`, `mmsi`, `segment_id` | 年、船舶、年度区间唯一标识 |
| `start_time_utc`, `end_time_utc`, `state` | 区间边界和五状态 |
| `from_port_call_id`, `from_port_group_id`, `from_port_group_name` | 前一确认港口调用/港口组 |
| `from_port_country_or_area` | 前一港口所在国家或地区 |
| `to_port_call_id`, `to_port_group_id`, `to_port_group_name` | 下一确认港口调用/港口组 |
| `to_port_country_or_area` | 下一港口所在国家或地区 |
| `ais_point_count`, `valid_speed_point_count` | 区间 AIS 点数和有效航速点数 |
| `min_speed_knots`, `mean_speed_knots`, `max_speed_knots` | 区间航速统计 |
| `max_observation_gap_seconds` | 区间内最大观测间隔 |
| `has_time_conflict`, `has_port_ambiguity` | 时间冲突和港口歧义证据 |
| `quality_flag`, `track_partition_id` | 质量标签和可追溯物理分区 |

不输出端点经纬度、持续时间和起终点直线距离。持续时间可由两个时间字段准确计算。
跨国家/地区港口组无法可靠细分时，国家/地区字段以 `; ` 连接全部成员值，同时在港口组目录中保留 `is_cross_border=true`。

## 7. 港口组更新边界

港口锚点只表达观测到的空间停留结构，港口组是独立目录映射。因此调整 `group_distance_km` 或删除 `Very Small` 港口时：

- 不重读原始 CSV；
- 不重建 canonical tracks、停留事件或点到 WPI 港口 geometry；
- 自动重建 `port_groups → calls → intervals → validation`；
- 保留原始港口成员、跨国家/地区标记、候选距离和歧义 margin 供效果验证。

重点检查：

```text
products/ports/port_coverage.parquet
products/validation/port_quality.csv
products/validation/harbor_size_quality.csv
products/validation/ambiguous_port_points.parquet
```

只有改变锚点网格、锚点支持阈值或港区距离半径时，才需要重建锚点或 geometry；轨迹仍不需要重跑。

## 8. 可删除内容

- `D:\ExtractAIS-v2-temp`：包含当前活动轨迹分区的去重/冲突中间文件和 DuckDB spill；运行未进行时可删除，成功的 `run-all` 会自动清空。
- `*/ingest_runs`：全部 tracks 完成后自动删除，也可用 `cleanup-staging`；命令会先验证全部轨迹检查点。
- `point_port_candidates`、`point_group_candidates`、`port_context`：属于可重建证据，但删除后 `status` 会识别输出缺失；后续阶段不能独立复用。
- `products/validation`：可重建报告。
- `products/quality_audit`：可重建的深度审计报告和断点 partials，不属于最终轨迹数据；删除后再次执行 `audit-quality` 会从头重建审计，但不会重跑主流程。
- canonical tracks、port calls 和最终 intervals 是主要保留结果，不应当作临时文件删除。

## 9. 只读诊断

运行任务期间另开 PowerShell：

```powershell
powershell -ExecutionPolicy Bypass -File scripts/diagnose_pipeline.ps1 `
  -Config configs/production.yaml -DurationMinutes 5 -IntervalSeconds 5
```

脚本只读取进程、物理盘性能、心跳、检查点和可用空间，不修改数据或进程状态。输出用于判断 CPU、内存、物理盘吞吐、队列以及当前活动子阶段。

## 10. 深度质量审计

现有 `state_summary.csv` 和 `port_quality.csv` 是快速验证报告：区间只要包含一个异常点，整段就会标记异常；港口调用只要包含一个歧义点，整次调用就会标记歧义。因此不能把其中的区间点数或调用数直接解释为错误率。

完整流程成功且 `validate` 已完成后运行：

```powershell
New-Item -ItemType Directory -Force logs | Out-Null
extractais --config configs/production.yaml audit-quality 2>&1 |
  Tee-Object -FilePath logs/audit-quality-v2.log
```

该命令不扫描原始 CSV 清单，也不修改 canonical tracks、港口调用、区间或 validation 结果。它按固定 MMSI 分区运行并写入同一个 `state.sqlite`，可随时 `Ctrl+C` 后用相同命令续跑。轨迹冲突阶段在 F/G 各运行一个任务；调用与区间证据位于产品盘，因此一次只运行一个重任务。空间保护在每个分区和全局合并前执行。

结果位于 `H:\ExtractAIS-v2\products\quality_audit`：

| 文件 | 用途 |
|---|---|
| `summary.json` | 口径、核心计数、路径和解释边界 |
| `time_conflict_summary.csv` | 真正的 AIS 点级同秒冲突比例 |
| `time_conflict_extent_bins.csv` | 同秒坐标包围盒对角距离代理量分层，区分近重复和远距离矛盾 |
| `time_conflict_examples.parquet` | 各距离层的高风险同秒样本 |
| `unknown_gap_duration.csv` | 按 6–12 小时、12–24 小时、1–3 天等统计中断时长 |
| `unknown_gap_coverage.csv` | 船舶年度活跃时间窗中的中断时间占比 |
| `state_transitions.csv` | 合并相邻同状态后得到的状态转移矩阵 |
| `interval_flag_propagation.csv` | 异常点经 `bool_or` 扩展到整段后的区间、点数和时长 |
| `ambiguity_assignment.csv` | 全部歧义候选点中位于确认港口调用内的覆盖比例 |
| `call_ambiguity_distribution.csv` | 每次调用中歧义点占比的分布，而非“一点即整次歧义” |
| `port_call_quality.csv` | 各港口组的调用级和点级歧义率、阈值敏感性及生命周期完整率 |
| `call_lifecycle.csv` | `ARRIVING/IN_PORT/DEPARTING` 是否围绕同一调用完整出现 |
| `competing_port_pairs.csv` | 歧义最集中的有向港口组对、距离、国家和 Harbor Size |
| `review_calls.parquet` | 高歧义比例、高歧义点数和低歧义对照调用的确定性样本 |
| `review_ambiguous_points.parquet` | 样本调用的代表 AIS 点及两个候选港口组坐标和距离 |

建议按以下顺序判断：先确认 `ambiguity_flag_mismatch_count` 接近零，验证调用歧义重算与原标签一致；再比较调用歧义率与调用内歧义点率；随后查看 `competing_port_pairs.csv` 的集中程度；最后在 QGIS/Python 中人工复核两个 `review_*.parquet`。`ambiguity_assignment.csv` 的未分配点通常是未形成确认调用的港口候选点，不要求达到 100%。这些报告只衡量内部一致性和证据集中度，没有人工标签时仍不能称为港口识别准确率。

完整算法和失效边界见 [docs/pipeline.md](docs/pipeline.md)。
