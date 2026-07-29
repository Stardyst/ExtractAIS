[CmdletBinding()]
param(
    [string]$Config = "configs/production.yaml",
    [double]$DurationMinutes = 5,
    [int]$IntervalSeconds = 5
)

$ErrorActionPreference = "Stop"
if ($DurationMinutes -le 0) { throw "DurationMinutes must be greater than zero." }
if ($IntervalSeconds -le 0) { throw "IntervalSeconds must be greater than zero." }
$configPath = (Resolve-Path -LiteralPath $Config).Path

$reader = @'
import json, sys
from pathlib import Path
import duckdb, extractais
from extractais.config import load_config
c = load_config(Path(sys.argv[1]))
print(json.dumps({
  "version": extractais.__version__, "duckdb": duckdb.__version__,
  "temp": str(c.storage.temp_root), "products": str(c.storage.products_root),
  "roots": [str(p) for p in (*c.storage.track_roots, c.storage.temp_root,
                              c.storage.products_root, *c.storage.evidence_roots)],
  "partitions": c.layout.track_partitions, "worker_threads": c.runtime.worker_threads,
  "worker_memory": c.runtime.worker_memory
}))
'@
$configInfo = ($reader | & python - $configPath) | ConvertFrom-Json
if ($LASTEXITCODE -ne 0) { throw "Activate the extractais Conda environment first." }

function Get-ProcessTreeIds {
    $all = @(Get-CimInstance Win32_Process)
    $roots = @($all | Where-Object {
        $_.CommandLine -match '(?i)extractais.*(ingest|tracks|ports|geometry|calls|intervals|validate|run-all)'
    })
    $ids = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($item in $roots) { [void]$ids.Add([int]$item.ProcessId) }
    do {
        $changed = $false
        foreach ($item in $all) {
            if ($ids.Contains([int]$item.ParentProcessId) -and -not $ids.Contains([int]$item.ProcessId)) {
                [void]$ids.Add([int]$item.ProcessId); $changed = $true
            }
        }
    } while ($changed)
    return @($ids)
}

function Get-FreeRows {
    foreach ($root in $configInfo.roots) {
        $qualifier = Split-Path -Qualifier $root
        $drive = Get-PSDrive -Name $qualifier.TrimEnd(':') -ErrorAction SilentlyContinue
        if ($drive) {
            [pscustomobject]@{ Root = $root; FreeTiB = [math]::Round($drive.Free / 1TB, 2) }
        }
    }
}

Write-Host "ExtractAIS v2 pipeline diagnostic (read-only)"
Write-Host "Config         : $configPath"
Write-Host "Versions       : ExtractAIS $($configInfo.version) / DuckDB $($configInfo.duckdb)"
Write-Host "Track layout   : $($configInfo.partitions) partitions"
Write-Host "Worker profile : $($configInfo.worker_threads) threads / $($configInfo.worker_memory)"
Write-Host ""

$previous = @{}
$samples = [math]::Ceiling($DurationMinutes * 60 / $IntervalSeconds)
$rows = @()
for ($sample = 0; $sample -lt $samples; $sample++) {
    $ids = @(Get-ProcessTreeIds)
    if ($ids.Count -eq 0) { Write-Host "No running ExtractAIS v2 process tree found."; break }
    $processes = @(Get-CimInstance Win32_Process | Where-Object { $ids -contains [int]$_.ProcessId })
    $cpu = 0.0; $read = 0.0; $write = 0.0; $private = 0.0
    foreach ($process in $processes) {
        $totalCpu = ([double]$process.KernelModeTime + [double]$process.UserModeTime) / 10000000
        $current = [pscustomobject]@{
            Cpu = $totalCpu; Read = [double]$process.ReadTransferCount; Write = [double]$process.WriteTransferCount
        }
        if ($previous.ContainsKey($process.ProcessId)) {
            $cpu += [math]::Max(0, ($current.Cpu - $previous[$process.ProcessId].Cpu) / $IntervalSeconds * 100)
            $read += [math]::Max(0, ($current.Read - $previous[$process.ProcessId].Read) / $IntervalSeconds / 1MB)
            $write += [math]::Max(0, ($current.Write - $previous[$process.ProcessId].Write) / $IntervalSeconds / 1MB)
        }
        $previous[$process.ProcessId] = $current
        $private += [double]$process.PrivatePageCount / 1GB
    }
    $disks = @(Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk |
        Where-Object { $_.Name -ne '_Total' })
    $diskRead = ($disks | Measure-Object -Property DiskReadBytesPersec -Sum).Sum / 1MB
    $diskWrite = ($disks | Measure-Object -Property DiskWriteBytesPersec -Sum).Sum / 1MB
    $busy = ($disks | Measure-Object -Property PercentDiskTime -Average).Average
    $queue = ($disks | Measure-Object -Property CurrentDiskQueueLength -Sum).Sum
    $heartbeat = Get-ChildItem -LiteralPath $configInfo.temp -Filter '*.json' -Recurse -ErrorAction SilentlyContinue |
        Sort-Object LastWriteTime -Descending | Select-Object -First 1
    $phase = if ($heartbeat) {
        try { (Get-Content -LiteralPath $heartbeat.FullName -Raw | ConvertFrom-Json).phase } catch { "unreadable heartbeat" }
    } else { "between tasks" }
    $row = [pscustomobject]@{
        Time = Get-Date -Format 'HH:mm:ss'; CPUCores = [math]::Round($cpu / 100, 2)
        PrivateGiB = [math]::Round($private, 1); ProcReadMiBps = [math]::Round($read, 1)
        ProcWriteMiBps = [math]::Round($write, 1); DiskReadMiBps = [math]::Round($diskRead, 1)
        DiskWriteMiBps = [math]::Round($diskWrite, 1); DiskBusyAvg = [math]::Round($busy, 0)
        DiskQueue = [math]::Round($queue, 1); Phase = $phase
    }
    $rows += $row; $row | Format-Table -AutoSize
    if ($sample + 1 -lt $samples) { Start-Sleep -Seconds $IntervalSeconds }
}

Write-Host "`nSummary"
if ($rows.Count -gt 0) {
    [pscustomobject]@{
        Samples = $rows.Count
        AverageCpuCores = [math]::Round(($rows | Measure-Object CPUCores -Average).Average, 2)
        PeakPrivateGiB = [math]::Round(($rows | Measure-Object PrivateGiB -Maximum).Maximum, 2)
        AverageProcessReadMiBps = [math]::Round(($rows | Measure-Object ProcReadMiBps -Average).Average, 2)
        AverageProcessWriteMiBps = [math]::Round(($rows | Measure-Object ProcWriteMiBps -Average).Average, 2)
        AverageDiskReadMiBps = [math]::Round(($rows | Measure-Object DiskReadMiBps -Average).Average, 2)
        AverageDiskWriteMiBps = [math]::Round(($rows | Measure-Object DiskWriteMiBps -Average).Average, 2)
        AverageDiskBusyPercent = [math]::Round(($rows | Measure-Object DiskBusyAvg -Average).Average, 2)
        PeakDiskQueue = [math]::Round(($rows | Measure-Object DiskQueue -Maximum).Maximum, 2)
    } | Format-List
}
Write-Host "Storage"
Get-FreeRows | Sort-Object Root -Unique | Format-Table -AutoSize
Write-Host "The script did not modify ExtractAIS data or process state."
