[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Config = "configs/production.yaml",

    [Parameter(Mandatory = $false)]
    [ValidateSet("Auto", "prepare", "stops", "ports", "calls", "intervals", "validate")]
    [string]$Stage = "Auto",

    [Parameter(Mandatory = $false)]
    [double]$DurationMinutes = 5,

    [Parameter(Mandatory = $false)]
    [int]$IntervalSeconds = 5
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "prepare_diagnostic_findings.ps1")

if ($DurationMinutes -le 0) { throw "DurationMinutes must be greater than zero." }
if ($IntervalSeconds -le 0) { throw "IntervalSeconds must be greater than zero." }

$configPath = (Resolve-Path -LiteralPath $Config).Path
$configReader = @'
import json
import sys
from pathlib import Path
import duckdb
import extractais
from extractais.config import load_config

config = load_config(Path(sys.argv[1]))
print(json.dumps({
    "work_root": str(config.storage.work_root),
    "temp_directory": str(config.storage.temp_directory),
    "mmsi_buckets": config.prepare.mmsi_buckets,
    "threads": config.runtime.threads,
    "memory_limit": config.runtime.memory_limit,
    "bucket_workers": config.runtime.bucket_workers,
    "bucket_threads": config.runtime.bucket_threads,
    "bucket_memory_limit": config.runtime.bucket_memory_limit,
    "bucket_temp_limit_gb": config.runtime.bucket_temp_limit_gb,
    "minimum_free_space_gb": config.runtime.minimum_free_space_gb,
    "extractais_version": extractais.__version__,
    "duckdb_version": duckdb.__version__,
}))
'@
$configJson = $configReader | & python - $configPath
if ($LASTEXITCODE -ne 0) {
    throw "Could not read the ExtractAIS config. Activate the extractais Conda environment first."
}
$configInfo = $configJson | ConvertFrom-Json
$workRoot = [System.IO.Path]::GetFullPath([string]$configInfo.work_root)
$tempRoot = [System.IO.Path]::GetFullPath([string]$configInfo.temp_directory)
$stagePattern = "prepare|stops|ports|calls|intervals|validate"

function Get-ExtractAISProcessTree {
    $all = @(Get-CimInstance Win32_Process)
    $launchers = @($all | Where-Object {
        ($_.Name -ieq "extractais.exe" -or $_.Name -ieq "python.exe") -and
        $_.CommandLine -match "(?i)(^|\s)($stagePattern)(\s|$)" -and
        ($Stage -eq "Auto" -or $_.CommandLine -match "(?i)(^|\s)$Stage(\s|$)")
    })
    if ($launchers.Count -eq 0) { return @() }

    $ids = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($launcher in $launchers) { [void]$ids.Add([int]$launcher.ProcessId) }
    do {
        $added = $false
        foreach ($process in $all) {
            if ($ids.Contains([int]$process.ParentProcessId) -and
                -not $ids.Contains([int]$process.ProcessId)) {
                [void]$ids.Add([int]$process.ProcessId)
                $added = $true
            }
        }
    } while ($added)
    return @($ids)
}

$allProcesses = @(Get-CimInstance Win32_Process)
$launcher = $allProcesses | Where-Object {
    ($_.Name -ieq "extractais.exe" -or $_.Name -ieq "python.exe") -and
    $_.CommandLine -match "(?i)(^|\s)($stagePattern)(\s|$)" -and
    ($Stage -eq "Auto" -or $_.CommandLine -match "(?i)(^|\s)$Stage(\s|$)")
} | Select-Object -First 1
if ($null -eq $launcher) {
    throw "No running ExtractAIS compute stage was found."
}
$match = [regex]::Match($launcher.CommandLine, "(?i)(^|\s)($stagePattern)(\s|$)")
$activeStage = $match.Groups[2].Value.ToLowerInvariant()

function Get-NewestItem {
    param([string[]]$Patterns)
    $items = @()
    foreach ($pattern in $Patterns) {
        $items += @(Get-ChildItem -Path $pattern -Force -ErrorAction SilentlyContinue)
    }
    return $items | Sort-Object LastWriteTime -Descending | Select-Object -First 1
}

$patterns = switch ($activeStage) {
    "prepare" {
        @(
            (Join-Path $workRoot "stage02_partitioned\year=*\month=*.tmp"),
            (Join-Path $workRoot "stage02_static\*.tmp.parquet"),
            (Join-Path $tempRoot "track-bucket-*")
        )
    }
    "stops" { @((Join-Path $workRoot "stage04_stops\mmsi_bucket=*\*.tmp.parquet")) }
    "ports" {
        @(
            (Join-Path $workRoot "stage05_ports.tmp"),
            (Join-Path $workRoot "stage05_ports\stop_port_matches\mmsi_bucket=*\*.tmp.parquet")
        )
    }
    "calls" { @((Join-Path $workRoot "stage06_port_calls\mmsi_bucket=*.tmp")) }
    "intervals" { @((Join-Path $workRoot "outputs\trajectory_intervals\.tmp\mmsi_bucket=*")) }
    "validate" { @((Join-Path $workRoot "outputs\validation.tmp")) }
}
$activeOutput = Get-NewestItem -Patterns $patterns
$activeOutputPath = if ($null -ne $activeOutput) { $activeOutput.FullName } else { $null }
$activeUnit = if ($null -ne $activeOutput) { $activeOutput.Name } else { "detecting" }

function Get-PathSnapshot {
    param([AllowNull()][string]$Path)
    if ([string]::IsNullOrWhiteSpace($Path) -or -not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{ FileCount = 0; Bytes = [int64]0 }
    }
    $item = Get-Item -LiteralPath $Path
    if (-not $item.PSIsContainer) {
        return [pscustomobject]@{ FileCount = 1; Bytes = [int64]$item.Length }
    }
    $files = @(Get-ChildItem -LiteralPath $Path -File -Recurse -ErrorAction SilentlyContinue)
    $sum = ($files | Measure-Object Length -Sum).Sum
    if ($null -eq $sum) { $sum = 0 }
    return [pscustomobject]@{ FileCount = $files.Count; Bytes = [int64]$sum }
}

$workDrive = [System.IO.Path]::GetPathRoot($workRoot).TrimEnd("\").TrimEnd(":")
$partition = Get-Partition -DriveLetter $workDrive -ErrorAction Stop
$diskNumber = [int]$partition.DiskNumber
$disk = Get-Disk -Number $diskNumber
$physicalDisk = Get-PhysicalDisk -ErrorAction SilentlyContinue |
    Where-Object { [string]$_.DeviceId -eq [string]$diskNumber } |
    Select-Object -First 1
$mediaType = if ($null -ne $physicalDisk) { [string]$physicalDisk.MediaType } else { "Unknown" }
$logicalProcessors = [int](Get-CimInstance Win32_ComputerSystem).NumberOfLogicalProcessors
$initialProcessIds = @(Get-ExtractAISProcessTree)
if ($initialProcessIds.Count -eq 0) { throw "The ExtractAIS process ended before sampling." }

$outputStart = Get-PathSnapshot -Path $activeOutputPath
$tempStart = Get-PathSnapshot -Path $tempRoot
$volumeStart = Get-Volume -DriveLetter $workDrive

Write-Host ""
Write-Host "ExtractAIS pipeline diagnostic (read-only)"
Write-Host "Config              : $configPath"
Write-Host "Stage               : $activeStage"
Write-Host "Active output       : $activeUnit"
Write-Host "Work root           : $workRoot"
Write-Host "DuckDB temp         : $tempRoot"
Write-Host "Physical disk       : $diskNumber / $($disk.FriendlyName) / $mediaType"
Write-Host "ExtractAIS / DuckDB : $($configInfo.extractais_version) / $($configInfo.duckdb_version)"
Write-Host "Global profile      : $($configInfo.threads) threads / $($configInfo.memory_limit)"
Write-Host "Bucket profile      : $($configInfo.bucket_workers) workers x $($configInfo.bucket_threads) threads / $($configInfo.bucket_memory_limit) each"
Write-Host "Bucket temp cap     : $($configInfo.bucket_temp_limit_gb) GiB each"
Write-Host "Free-space reserve  : $($configInfo.minimum_free_space_gb) GiB"
Write-Host "Initial process IDs : $($initialProcessIds -join ', ')"
Write-Host ""
Write-Host "Time      CPU%  Cores  Private  Avail  ProcR  ProcW  DiskR  DiskW  Busy%  Queue  FreeTiB"

$sampleCount = [Math]::Max(1, [Math]::Ceiling($DurationMinutes * 60 / $IntervalSeconds))
$samples = [System.Collections.Generic.List[object]]::new()
for ($index = 0; $index -lt $sampleCount; $index++) {
    $processIds = @(Get-ExtractAISProcessTree)
    if ($processIds.Count -eq 0) { Write-Host "Process ended during sampling."; break }
    $processCounters = @(Get-CimInstance Win32_PerfFormattedData_PerfProc_Process |
        Where-Object { $processIds -contains [int]$_.IDProcess })
    if ($processCounters.Count -eq 0) { Write-Host "Performance counters disappeared."; break }
    $diskCounter = Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk |
        Where-Object { $_.Name -match "^$diskNumber(\s|$)" } | Select-Object -First 1
    $memoryCounter = Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory
    $volume = Get-Volume -DriveLetter $workDrive
    $cpuRaw = [double](($processCounters | Measure-Object PercentProcessorTime -Sum).Sum)
    $sample = [pscustomobject]@{
        Timestamp = Get-Date
        CpuPercent = if ($logicalProcessors) { $cpuRaw / $logicalProcessors } else { 0 }
        CpuCores = $cpuRaw / 100
        PrivateGiB = [double](($processCounters | Measure-Object WorkingSetPrivate -Sum).Sum) / 1GB
        AvailableMemoryGiB = [double]$memoryCounter.AvailableMBytes / 1024
        Handles = [double](($processCounters | Measure-Object HandleCount -Sum).Sum)
        Threads = [double](($processCounters | Measure-Object ThreadCount -Sum).Sum)
        ProcessReadMiBps = [double](($processCounters | Measure-Object IOReadBytesPersec -Sum).Sum) / 1MB
        ProcessWriteMiBps = [double](($processCounters | Measure-Object IOWriteBytesPersec -Sum).Sum) / 1MB
        DiskReadMiBps = [double]$diskCounter.DiskReadBytesPersec / 1MB
        DiskWriteMiBps = [double]$diskCounter.DiskWriteBytesPersec / 1MB
        DiskBusyPercent = [double]$diskCounter.PercentDiskTime
        DiskQueue = [double]$diskCounter.AvgDiskQueueLength
        FreeTiB = [double]$volume.SizeRemaining / 1TB
    }
    $samples.Add($sample)
    Write-Host ("{0:HH:mm:ss} {1,5:N1} {2,6:N2} {3,7:N1} {4,6:N1} {5,6:N1} {6,6:N1} {7,6:N1} {8,6:N1} {9,6:N0} {10,6:N1} {11,8:N2}" -f `
        $sample.Timestamp, $sample.CpuPercent, $sample.CpuCores, $sample.PrivateGiB,
        $sample.AvailableMemoryGiB, $sample.ProcessReadMiBps, $sample.ProcessWriteMiBps,
        $sample.DiskReadMiBps, $sample.DiskWriteMiBps, $sample.DiskBusyPercent,
        $sample.DiskQueue, $sample.FreeTiB)
    if ($index -lt $sampleCount - 1) { Start-Sleep -Seconds $IntervalSeconds }
}
if ($samples.Count -eq 0) { throw "No performance samples were collected." }

$outputEnd = Get-PathSnapshot -Path $activeOutputPath
$tempEnd = Get-PathSnapshot -Path $tempRoot
$volumeEnd = Get-Volume -DriveLetter $workDrive
function Get-Average([string]$Property) {
    return [double](($samples | Measure-Object -Property $Property -Average).Average)
}
function Get-Maximum([string]$Property) {
    return [double](($samples | Measure-Object -Property $Property -Maximum).Maximum)
}
$summary = [pscustomobject]@{
    Samples = $samples.Count
    AverageCpuPercent = Get-Average "CpuPercent"
    AverageCpuCores = Get-Average "CpuCores"
    PeakPrivateGiB = Get-Maximum "PrivateGiB"
    MinimumAvailableMemoryGiB = [double](($samples | Measure-Object AvailableMemoryGiB -Minimum).Minimum)
    PeakHandles = Get-Maximum "Handles"
    PeakThreads = Get-Maximum "Threads"
    AverageProcessReadMiBps = Get-Average "ProcessReadMiBps"
    AverageProcessWriteMiBps = Get-Average "ProcessWriteMiBps"
    AverageDiskReadMiBps = Get-Average "DiskReadMiBps"
    AverageDiskWriteMiBps = Get-Average "DiskWriteMiBps"
    AverageDiskBusyPercent = Get-Average "DiskBusyPercent"
    AverageDiskQueue = Get-Average "DiskQueue"
    PeakDiskQueue = Get-Maximum "DiskQueue"
    OutputGrowthGiB = ($outputEnd.Bytes - $outputStart.Bytes) / 1GB
    OutputFileGrowth = $outputEnd.FileCount - $outputStart.FileCount
    OutputFilesAtEnd = $outputEnd.FileCount
    TempGrowthGiB = ($tempEnd.Bytes - $tempStart.Bytes) / 1GB
    FreeSpaceChangeGiB = ([double]$volumeEnd.SizeRemaining - [double]$volumeStart.SizeRemaining) / 1GB
}

Write-Host ""
Write-Host "Summary"
$summary | Format-List
Write-Host "Findings"
$findings = @(Get-PrepareDiagnosticFindings -Summary $summary `
    -Phase $activeStage -MmsiBuckets ([int]$configInfo.mmsi_buckets))
foreach ($finding in $findings) { Write-Host "- $finding" }
Write-Host ""
Write-Host "The script did not modify ExtractAIS data or process state."
