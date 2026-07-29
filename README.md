# ExtractAIS

ExtractAIS 将按日保存的超大规模 AIS CSV 转换为按 MMSI、时间排序的年度船舶状态区间。处理过程先分离动态/静态报文，再识别停留、港口锚点、港口调用和五种运行状态。所有批量计算由 DuckDB 执行，Python 只负责阶段调度，不逐行处理 AIS。

## 1. 环境安装

在仓库根目录执行：

```powershell
conda env create -f environment.yml
conda activate extractais
python -m pip install -e .
extractais --help
```

已经创建过 `extractais` 环境时，使用下面的命令更新本地安装：

```powershell
git pull origin main
conda activate extractais
python -m pip install -e .
python -c "import extractais; print(extractais.__version__)"
```

当前版本应显示 `1.6.0`。

## 2. 准备生产配置

不要直接修改示例文件。先建立本机配置：

```powershell
Copy-Item configs/production.example.yaml configs/production.yaml
notepad configs/production.yaml
```

默认生产路径对应当前数据主机：

```yaml
input:
  raw_root: "E:/AIS2021-2022"
  year_directories:
    2021: "2021"
    2022: "2022"
  ports_csv: "E:/AIS2021-2022/UpdatedPub150.csv"
storage:
  work_root: "E:/AIS2021-2022/derived"
  temp_directory: "E:/AIS2021-2022/derived/tmp"
```

`configs/production.yaml` 已由 `.gitignore` 排除。当前 128 GB、24 核/32 线程、单块 HDD 主机使用两个资源档位：

```yaml
runtime:
  threads: 20
  memory_limit: "90GB"
  bucket_workers: 1
  bucket_threads: 8
  bucket_memory_limit: "80GB"
  bucket_temp_limit_gb: 256
  enable_progress: true
  minimum_free_space_gb: 500
```

月度分区、静态表、全局港口锚点和验证报告使用全局档位；轨迹工作单元、停留、停留到港口匹配、港口调用和区间构建使用桶档位。阶段 02 仍使用 256 个粗 MMSI 桶，但阶段 03 会按 `hash(mmsi)` 将每个粗桶自适应细分，目标为每片不超过约 512 MiB 压缩输入；同一 MMSI 永远只在一片中。窗口线程数还会受实际内存上限约束。单 HDD 保持 `bucket_workers: 1`，避免随机 I/O；80 GB 档位为异常数据分布和 DuckDB 非缓冲区分配保留余量。

生产配置还必须为每个 MMSI 桶保留一个月度分区写入器：

```yaml
prepare:
  mmsi_buckets: 256
  partition_write_max_open_files: 256
```

`partition_write_max_open_files` 小于 `mmsi_buckets` 会让 DuckDB 反复关闭并重建分区文件，生产月份可膨胀到上万个小文件，因此新版会直接拒绝这种配置。

## 3. 先检查输入

```powershell
extractais --config configs/production.yaml inventory
```

该命令检查日期、重复日期和总字节数，并写入：

```text
E:/AIS2021-2022/derived/manifests/input_inventory.json
```

全量运行前必须检查输出中的 `missing_dates` 和 `duplicate_dates`。缺失日期可以继续处理，但会形成 AIS 中断；重复日期必须先确认应保留哪个源文件。

## 4. 两日试运行

建议先在正式 `work_root` 之外设置一个试运行目录，随后执行：

```powershell
extractais --config configs/local-validation.yaml split `
  --input-file sample_2021-01-23_1100.csv `
  --input-file sample_2022-01-23_1100.csv
```

1100 行样本适合检查字段和 CSV 解析，但不足以稳定学习港口锚点。用于测量吞吐量和磁盘占用时，应在单独配置中选择至少一个完整月份，然后执行完整流程。

## 5. 运行完整流程

最简单的方式是执行：

```powershell
New-Item -ItemType Directory -Force logs | Out-Null
extractais --config configs/production.yaml run-all 2>&1 |
  Tee-Object -FilePath logs/run-all.log
```

`run-all` 按顺序运行 `split -> prepare -> stops -> ports -> calls -> intervals -> validate`。已经完成且输入、参数、输出均未变化的检查点会被跳过。

也可以逐阶段执行，便于每个阶段结束后检查磁盘和质量：

```powershell
extractais --config configs/production.yaml split
extractais --config configs/production.yaml prepare
extractais --config configs/production.yaml stops
extractais --config configs/production.yaml ports
extractais --config configs/production.yaml calls
extractais --config configs/production.yaml intervals
extractais --config configs/production.yaml validate
```

### 各阶段的工作

| 命令 | 处理内容 | 恢复粒度 |
|---|---|---|
| `split` | 每个 CSV 只解析一次；动态报文与静态报文立即分离为 Parquet | 每日文件 |
| `prepare` | 静态信息压缩到每 MMSI 一行；动态信息按月一次扫描并粗分桶；再自适应细分、去重、排序和标记异常 | 月、静态表、轨迹源桶/细片 |
| `stops` | 从低速、连续、有限空间范围的位置点中提取停留事件 | 轨迹细片 |
| `ports` | 读取 WPI，合并无法区分的近邻港口，建立多圆锚点，再按细片匹配停留事件 | 全局目录、轨迹细片 |
| `calls` | 精确计算位置到锚点距离，保留候选港口，使用独立检测的停留事件确认港口调用 | 轨迹细片 |
| `intervals` | 识别五状态、插入长中断、压缩连续状态并直接写入年度最终目录 | 轨迹细片 |
| `validate` | 汇总港口覆盖、调用、歧义、Harbor Size 和状态质量 | 整个验证报告 |

## 6. 中断和恢复

可以使用 `Ctrl+C` 停止。恢复时执行同一条命令：

```powershell
extractais --config configs/production.yaml run-all
```

恢复规则如下：

- 已完成的日、月、轨迹源桶或轨迹细片直接跳过。
- `prepare` 在每个轨迹细片原子提交后立即写检查点；中断后只重建缺失或不完整的细片。
- 每个重型工作单元在独立操作系统子进程中运行；单元结束后进程退出，DuckDB 原生分配器、缓存、线程和句柄由操作系统整体回收。
- 子进程返回成功结果后只等待有限时间退出；解释器清理不会再让主进度永久卡在 `worker shutdown`。
- 一个桶失败后停止派发新桶，但允许已经运行的桶完成并提交，再报告原始错误。
- 最终文件先写为临时产物，成功后再原子替换；中断不会把半个 Parquet 标记为完成。
- 检查点位于 `derived/manifests/*.json`，包含输入身份、阶段参数哈希、Git commit、子进程 PID、耗时和输出路径。
- `--force` 会重建该命令所有已完成工作单元，只在确认需要重算时使用。

子进程隔离边界与恢复粒度一致：`split` 为每日文件，`prepare` 为月/静态表/轨迹源桶及其细片，`stops`、停留到港口匹配、`calls` 和 `intervals` 为轨迹细片，港口锚点和 `validate` 为全局单元。全局单元一次运行一个进程；细片阶段最多同时运行 `bucket_workers` 个进程，每个进程严格使用桶档位的线程、内存和独立临时目录。

### 从旧版本继续现有任务

先停止旧进程，再在仓库根目录执行：

```powershell
git pull origin main
conda activate extractais
python -m pip install -e .
python -c "import extractais; print(extractais.__version__)"
extractais --config configs/production.yaml status
```

版本应为 `1.6.0`。不需要删除 `derived`，也不要加 `--force`。`configs/production.yaml` 不受 Git 管理，因此升级时必须手工确认 `bucket_workers: 1`、`bucket_threads: 8` 和 `bucket_memory_limit: "80GB"`。若配置中仍是 `partition_write_max_open_files: 100`，也必须改为 `256`。

当前是在 `prepare` 中途暂停时，继续执行：

```powershell
extractais --config configs/production.yaml prepare
```

首次使用 1.6.0 执行 `prepare` 时，程序会写入轨迹布局标记，清理旧版 `stage03_tracks` 以及阶段 04 以后依赖旧桶编号的派生目录和 manifest。已经完成的 split、24 个月分区和静态船舶表保持有效，不会重新计算；后续结果将基于新轨迹细片重建，杜绝混入旧桶文件。不要改变 `prepare.mmsi_buckets`，当前应继续保持 256。每个粗桶先流式细分，再逐片物化精确去重、同时间冲突和序列计算；细片成功后立即写入独立检查点。

旧版异常退出可能在 `derived/tmp/worker-*` 留下临时文件。只有在确认没有 ExtractAIS 进程运行后，才执行一次清理：

```powershell
$running = Get-CimInstance Win32_Process |
  Where-Object { $_.CommandLine -match "extractais" }
if ($running) { throw "ExtractAIS is still running; temporary files were not removed." }

$tempRoot = (Resolve-Path "E:/AIS2021-2022/derived/tmp").Path
Get-ChildItem -LiteralPath $tempRoot -Directory -Filter "worker-*" |
  Where-Object { $_.FullName.StartsWith($tempRoot + [IO.Path]::DirectorySeparatorChar) } |
  Remove-Item -Recurse -Force
```

新版桶任务不再依赖 PID 临时目录；中断后的同一桶重试会清理自己的确定性目录。

## 7. 输出位置

所有派生数据默认位于 `E:/AIS2021-2022/derived`：

```text
derived/
  manifests/                         # 所有恢复检查点
  stage01_split/
    dynamic/year=YYYY/month=MM/day=DD/part.parquet
    static/year=YYYY/month=MM/day=DD/part.parquet
  stage02_partitioned/               # 按月建立的临时 MMSI 分桶
  stage02_static/vessels.parquet     # 每 MMSI 一行的最新静态信息
  stage03_tracks/mmsi_bucket=NNNNN/part.parquet  # 自适应轨迹细片
  stage04_stops/mmsi_bucket=NNNNN/part.parquet
  stage05_ports/
    anchors.parquet
    anchor_tiles.parquet
    stop_port_matches/mmsi_bucket=NNNNN/part.parquet
  stage06_port_calls/mmsi_bucket=NNNNN/
    candidates.parquet
    port_context.parquet
    port_calls.parquet
  outputs/
    trajectory_intervals/year=YYYY/mmsi_bucket=NNNNN.parquet
    validation/
```

正式分析应读取：

```text
E:/AIS2021-2022/derived/outputs/trajectory_intervals/year=2021/*.parquet
E:/AIS2021-2022/derived/outputs/trajectory_intervals/year=2022/*.parquet
```

## 8. 最终区间字段

年度区间表包含：

| 字段 | 含义 |
|---|---|
| `year` | 输出年份 |
| `mmsi` | 船舶 MMSI |
| `segment_id` | MMSI/年份内稳定排序的区间 ID |
| `start_time_utc`, `end_time_utc` | 该状态的观测时间边界 |
| `state` | `IN_PORT`、`DEPARTING`、`OCEAN`、`ARRIVING` 或 `UNKNOWN_GAP` |
| `from_port_group_id`, `from_port_group_name` | 最近一次已确认港口调用；进港后自动更新 |
| `to_port_group_id`, `to_port_group_name` | 下一次已确认港口调用 |
| `point_count` | 构成区间的有效 AIS 位置数；中断为 0 |
| `max_gap_seconds` | 区间内最大观测间隔，用于审计 |
| `quality_flag` | `OBSERVED`、`NO_AIS`、`PORT_AMBIGUOUS` 或 `TIME_CONFLICT` |
| `mmsi_bucket` | 阶段 03 的物理轨迹细片号，便于并行读取和问题追踪；不再等同于原始 `mmsi % 256` 粗桶号 |

最终表不包含区间起止经纬度、持续时间或直线距离。持续时间可由两个 UTC 字段计算；轨迹距离应基于完整位置序列另行计算。

## 9. 港口识别和验证

默认空间规则：进入 3 km、离开 4 km、接近/离开阶段 10 km。圆心不是单一 WPI 坐标，而是全年停留事件形成的多个 0.005 度锚点。中心距离 4 km 内的 WPI 港口通过连通分量合并为同一个 `port_group_id`，避免在密集港区强行区分。

关键审计产物：

| 文件 | 用途 |
|---|---|
| `stage05_ports/port_catalog.parquet` | WPI 港口到 `port_group_id` 的映射 |
| `stage05_ports/anchors.parquet` | 每个锚点的位置、停留次数、船数、最近 WPI 距离 |
| `stage05_ports/unmatched_anchor_candidates.parquet` | 有停留支持但未分配给 WPI 的区域 |
| `stage05_ports/port_coverage.parquet` | 每个港口组的锚点覆盖情况 |
| `stage05_ports/stop_port_matches/*/part.parquet` | 每个停留事件到最近港口锚点的分桶匹配及距离 |
| `stage06_port_calls/*/candidates.parquet` | 每个近港位置的全部候选港口、精确距离、名次和原始源标签 |
| `stage06_port_calls/*/port_context.parquet` | 每个近港位置的第一、第二候选及歧义距离，供调用、区间和验证复用 |
| `stage06_port_calls/*/port_calls.parquet` | 已确认港口调用及停留、`at_dock`、歧义证据 |
| `outputs/validation/port_quality.csv` | 每个港口组的覆盖、调用、歧义和审查状态 |
| `outputs/validation/harbor_size_quality.csv` | 按 Large/Medium/Small/Very Small 分层的效果 |
| `outputs/validation/ambiguous_port_points.parquet` | 第一、第二候选距离差小于阈值的位置样本 |
| `outputs/validation/state_summary.csv` | 各年份、状态、质量标记的区间数量 |

是否删除 `Very Small` 港口应根据 `harbor_size_quality.csv` 和港口级报告决定。决定删除后，在配置中设置：

```yaml
ports:
  excluded_harbor_sizes: ["Very Small"]
```

随后从 `ports` 开始依次运行后四个命令。阶段哈希会自动触发重算，不需要重新解析 CSV：

```powershell
extractais --config configs/production.yaml ports
extractais --config configs/production.yaml calls
extractais --config configs/production.yaml intervals
extractais --config configs/production.yaml validate
```

## 10. 中断阈值和状态规则

第一版对超过 `intervals.unknown_gap_hours` 的相邻有效 AIS 位置统一输出 `UNKNOWN_GAP`，默认阈值为 6 小时，不推测中断期间状态。短间隔仍可属于同一事件。

已确认港口调用内部为 `IN_PORT`；离开最近港口 10 km 范围内为 `DEPARTING`；进入下一港口 10 km 范围内为 `ARRIVING`；其余有效位置为 `OCEAN`。港口调用必须进入 3 km 范围，并且得到独立检测的停留事件支持。源 `at_dock` 和港口名称标签只用于验证，不参与确认，因此只从港口附近经过不会自动成为港口调用。

## 11. 性能和磁盘

- `split` 按原始字节显示总进度；每个日期完成后显示该日压缩率和工作盘剩余 TiB。
- `split` 每天只扫描一次规范化结果用于汇总，再分别写入动态和静态 Parquet；`COPY` 返回值直接作为有效行数，不再额外全表计数。
- 恢复时总进度从已完成文件的真实字节数开始，跳过旧日期不会再产生虚假的 TB/s 瞬时速度。
- 每个重型工作单元由独立子进程执行，修复了长时间运行中 DuckDB 连接虽然关闭、但同一 Python 进程的原生分配器和缓存仍持续累积而导致的吞吐衰减。
- `prepare` 的月度中间层使用 256 个粗 MMSI 桶，使当前实测月度输入平均每桶约 119 MiB；月度分区同时保持 256 个写入器，使每个非空桶只生成一个 Parquet 文件。
- 全年粗桶最大实测为 13.33 GiB 压缩数据，无法直接执行全局窗口。阶段 03 根据粗桶大小和每线程可用内存将其细分；13.33 GiB 桶在生产配置下分为 32 片，平均粗桶通常分为 8 片。同一 MMSI 不跨片，后续所有阶段继续使用这些细片。
- 生产 `row_group_size` 为 250000，降低分区写入峰值内存，并为后续单桶并行扫描保留足够的 row group。
- 在同一份 1000 万行、256 桶合成输入上，`partition_write_max_open_files=100` 用时 13.99 秒并生成 5120 个文件；设置为 256 后用时 8.03 秒且只生成 256 个文件。生产诊断中观察到的 17754 个文件正是相同的写入器轮换问题。
- 第一个月尚未提交时，进度条每 30 秒更新临时输出 GiB 和文件数。月度提交前还会强制检查文件数不得超过 MMSI 桶数。
- DuckDB 内部进度条已关闭，因为其算子百分比不包含阻塞算子的收尾、Parquet flush 和文件替换，曾把剩余时间低估到实际值的约四分之一。
- 主进度条的 ETA 只使用已经完整提交的相同工作单元。没有实测样本时显示 `ETA calibrating`；随后采用最近 12 个单元的中位吞吐率，并在尾部按实际剩余工人数修正。
- `prepare` 的轨迹进度不再只按 256 个粗桶整数计数。重分片期间根据已写字节推进，随后按当前细片的去重、冲突检测、序列计算和提交阶段产生单调递增的分数进度；粗桶完成后才加入 ETA 样本。百分比保留两位小数，因此第一只生产大桶内部也会显示变化；进度尾部会显示类似 `0000:shard 7/32 sequencing and writing`。
- 后续 `stops`、停留匹配、`calls` 和 `intervals` 直接以较小轨迹细片为工作单元，因此进度会在每个细片提交后持续推进，而不是等待一个十几 GiB 大桶完成。
- 动态分桶按月只扫描一次，不会为 256 个桶重复扫描全年数据。
- 停留到港口匹配按轨迹细片保存，不再生成一个全局匹配大表；港口调用生成一次紧凑 `port_context`，区间和验证复用它；区间直接写最终年度文件，不再保留并再次扫描一份 `stage07_intervals`。
- 细分数据只在当前粗桶的临时目录中存在，完成后删除；不会永久复制一份 836.64 GiB 的阶段 02 分区。阶段 03 最终总量仍按实测比例预计约 865 GiB。
- `stage01_split`、`stage02_partitioned` 和 `stage03_tracks` 会短期同时存在。4.48 TB 是否足够应以完整月份试运行的实际压缩比为准。
- `temp_directory` 必须和 `work_root` 位于容量充足的工作盘；不要指向系统盘。
- 全局任务要求 `free >= minimum_free_space + estimated_output`。桶任务还会预留 `bucket_workers * bucket_temp_limit_gb` 和活动桶的预计输出；默认固定预留为 `500 + 1 * 256 = 756 GiB`。不满足时会在启动下一个子进程前报 `Storage guard stopped`，已提交检查点不受损。
- 不要手工删除阶段目录后再执行 `run-all`。当前版本把中间产物视为可复现检查点，删除后会按依赖关系重建。

当前生产盘前 37 天的实测数据为：309.72 GiB CSV 生成 45.63 GiB 动态 Parquet 和 5.38 GiB 静态 Parquet，`split` 压缩率为 16.47%，据此估计完整 `split` 约 1.03 TiB。后续候选港口规模依赖真实近港密度，不能仅由 CSV 压缩率可靠外推；应以每阶段 manifest 的 `source_bytes`、`output_bytes` 和空间保护结果为准。

### 诊断任意阶段性能

在 `prepare`、`stops`、`ports`、`calls`、`intervals` 或 `validate` 正在运行时，另开一个已激活 `extractais` 环境的 PowerShell 窗口运行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File scripts/diagnose_pipeline.ps1 `
  -Config configs/production.yaml `
  -Stage Auto `
  -DurationMinutes 5 `
  -IntervalSeconds 5
```

脚本只读取配置、临时输出和 Windows CIM 性能计数器，不停止进程、不修改派生数据，也不创建报告文件。它会自动识别阶段并输出整个进程树的 CPU/内存/句柄、物理盘吞吐和队列、DuckDB 临时目录增长、输出增长及瓶颈判断。需要限定阶段时可把 `Auto` 改为具体命令名。`diagnose_prepare.ps1` 仍可用于查看 `prepare` 的月度分区细节。

从旧版升级后，只有在新版 `intervals` 和 `validate` 都成功完成并核对最终输出后，旧的 `derived/stage07_intervals` 和旧单文件 `derived/stage05_ports/stop_port_matches.parquet` 才是未被读取的冗余产物；新版不会自动删除既有数据。

检查工作盘剩余空间：

```powershell
Get-Volume -DriveLetter E | Select-Object DriveLetter, SizeRemaining, Size
```

## 12. 测试和 Git

```powershell
conda activate extractais
pytest
git status
```

仓库只跟踪代码、配置示例、测试和文档。CSV、Parquet、工作目录、manifest、日志和本机生产配置不会上传到 Git。

更详细的数据契约和算法说明见 [`docs/pipeline.md`](docs/pipeline.md)。
