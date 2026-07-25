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
conda activate extractais
python -m pip install -e .
```

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

`configs/production.yaml` 已由 `.gitignore` 排除。路径、线程数和内存限制可以按主机调整；生产主机的 128 GB 内存建议保持 `threads: 20`、`memory_limit: "100GB"`，为系统和文件缓存保留余量。

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
| `prepare` | 静态信息压缩到每 MMSI 一行；动态信息按月一次扫描并分桶；每桶按 MMSI/时间排序、去重、标记异常 | 月、MMSI 桶 |
| `stops` | 从低速、连续、有限空间范围的位置点中提取停留事件 | MMSI 桶 |
| `ports` | 读取 WPI，合并无法区分的近邻港口，建立停留支持的多圆锚点及空间索引 | 整个港口目录 |
| `calls` | 精确计算位置到锚点距离，保留候选港口，使用独立检测的停留事件确认港口调用 | MMSI 桶 |
| `intervals` | 识别五状态、插入长中断、压缩连续状态并按年份输出 | MMSI 桶、年度导出 |
| `validate` | 汇总港口覆盖、调用、歧义、Harbor Size 和状态质量 | 整个验证报告 |

## 6. 中断和恢复

可以使用 `Ctrl+C` 停止。恢复时执行同一条命令：

```powershell
extractais --config configs/production.yaml run-all
```

恢复规则如下：

- 已完成的日、月或 MMSI 桶直接跳过。
- 正在执行但尚未完成的最小工作单元会从头重算。
- 最终文件先写为临时产物，成功后再原子替换；中断不会把半个 Parquet 标记为完成。
- 检查点位于 `derived/manifests/*.json`，包含输入身份、阶段参数哈希、Git commit、耗时和输出路径。
- `--force` 会重建该命令所有已完成工作单元，只在确认需要重算时使用。

查看目前进度和最终路径：

```powershell
extractais --config configs/production.yaml status
```

从早期 `0.1.0` 版本升级时，`split` 的哈希由全局配置改为阶段配置。旧版已经完成的少量日期会重新处理一次，此后修改港口参数不会使 CSV 分离失效。

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
  stage03_tracks/mmsi_bucket=NNNN/part.parquet
  stage04_stops/mmsi_bucket=NNNN/part.parquet
  stage05_ports/
  stage06_port_calls/mmsi_bucket=NNNN/
  stage07_intervals/mmsi_bucket=NNNN/part.parquet
  outputs/
    trajectory_intervals/year=YYYY/*.parquet
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
| `mmsi_bucket` | 物理分桶号，便于并行读取和问题追踪 |

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
| `stage06_port_calls/*/candidates.parquet` | 每个近港位置的全部候选港口、精确距离、名次和原始源标签 |
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

- `split` 按原始字节显示总进度，DuckDB 同时显示当前查询进度。
- 后续阶段按月或 MMSI 桶显示进度，单桶可以使用磁盘临时空间完成外部排序。
- 动态分桶按月只扫描一次，不会为 512 个桶重复扫描全年数据。
- `stage01_split`、`stage02_partitioned` 和 `stage03_tracks` 会短期同时存在。4.48 TB 是否足够应以完整月份试运行的实际压缩比为准。
- `temp_directory` 必须和 `work_root` 位于容量充足的工作盘；不要指向系统盘。
- 不要手工删除阶段目录后再执行 `run-all`。当前版本把中间产物视为可复现检查点，删除后会按依赖关系重建。

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
