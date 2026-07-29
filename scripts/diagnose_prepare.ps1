[CmdletBinding()]
param(
    [Parameter(Mandatory = $false)]
    [string]$Config = "configs/production.yaml",

    [Parameter(Mandatory = $false)]
    [double]$DurationMinutes = 5,

    [Parameter(Mandatory = $false)]
    [int]$IntervalSeconds = 5
)

$ErrorActionPreference = "Stop"
. (Join-Path $PSScriptRoot "prepare_diagnostic_findings.ps1")

if ($DurationMinutes -le 0) {
    throw "DurationMinutes must be greater than zero."
}
if ($IntervalSeconds -le 0) {
    throw "IntervalSeconds must be greater than zero."
}

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
    "partition_write_max_open_files": config.prepare.partition_write_max_open_files,
    "threads": config.runtime.bucket_threads,
    "memory_limit": config.runtime.bucket_memory_limit,
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
$manifestRoot = Join-Path $workRoot "manifests"
$splitManifestPath = Join-Path $manifestRoot "split.json"
$prepareManifestPath = Join-Path $manifestRoot "prepare.json"

if (-not (Test-Path -LiteralPath $splitManifestPath)) {
    throw "Split manifest not found: $splitManifestPath"
}

$splitManifest = Get-Content -LiteralPath $splitManifestPath -Raw | ConvertFrom-Json
if (Test-Path -LiteralPath $prepareManifestPath) {
    $prepareManifest = Get-Content -LiteralPath $prepareManifestPath -Raw | ConvertFrom-Json
} else {
    $prepareManifest = [pscustomobject]@{ items = [pscustomobject]@{} }
}

function Get-ManifestItem {
    param([string]$Name)
    $property = $prepareManifest.items.PSObject.Properties[$Name]
    if ($null -eq $property) {
        return $null
    }
    return $property.Value
}

function Test-ManifestItemComplete {
    param([string]$Name)
    $item = Get-ManifestItem -Name $Name
    if ($null -eq $item -or $item.status -ne "complete") {
        return $false
    }
    if ($null -ne $item.output) {
        return Test-Path -LiteralPath $item.output
    }
    $outputs = @($item.outputs)
    return $outputs.Count -gt 0 -and @(
        $outputs | Where-Object { -not (Test-Path -LiteralPath $_) }
    ).Count -eq 0
}

function Get-PathSnapshot {
    param([string]$Path)
    if (-not (Test-Path -LiteralPath $Path)) {
        return [pscustomobject]@{ FileCount = 0; Bytes = [int64]0 }
    }
    $item = Get-Item -LiteralPath $Path
    if (-not $item.PSIsContainer) {
        return [pscustomobject]@{ FileCount = 1; Bytes = [int64]$item.Length }
    }
    $files = @(Get-ChildItem -LiteralPath $Path -File -Recurse -ErrorAction SilentlyContinue)
    $sum = ($files | Measure-Object -Property Length -Sum).Sum
    if ($null -eq $sum) {
        $sum = 0
    }
    return [pscustomobject]@{ FileCount = $files.Count; Bytes = [int64]$sum }
}

$splitRecords = @($splitManifest.files.PSObject.Properties.Value | Where-Object { $_.status -eq "complete" })
$months = @($splitRecords | ForEach-Object { $_.date.Substring(0, 7) } | Sort-Object -Unique)
$phase = $null
$unit = $null
$sourceBytes = [int64]0
$activeOutputPath = $null

$partitionRoot = Join-Path $workRoot "stage02_partitioned"
if (Test-Path -LiteralPath $partitionRoot) {
    $liveMonthTemp = Get-ChildItem -LiteralPath $partitionRoot -Directory -Recurse -ErrorAction SilentlyContinue |
        Where-Object { $_.Name -match "^month=\d{2}\.tmp$" } |
        Sort-Object -Property LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -ne $liveMonthTemp) {
        $yearText = $liveMonthTemp.Parent.Name -replace "^year=", ""
        $monthText = $liveMonthTemp.Name -replace "^month=", "" -replace "\.tmp$", ""
        $phase = "partition_month"
        $unit = "$yearText-$monthText"
        $sourceBytes = [int64](($splitRecords | Where-Object { $_.date.StartsWith($unit) } |
            Measure-Object -Property dynamic_output_bytes -Sum).Sum)
        $activeOutputPath = $liveMonthTemp.FullName
    }
}

if ($null -eq $phase) {
    $liveStaticTemp = Join-Path $workRoot "stage02_static\vessels.tmp.parquet"
    if (Test-Path -LiteralPath $liveStaticTemp) {
        $phase = "compact_static"
        $unit = "static"
        $sourceBytes = [int64](($splitRecords | Measure-Object -Property static_output_bytes -Sum).Sum)
        $activeOutputPath = $liveStaticTemp
    }
}

if ($null -eq $phase) {
    $liveTrackProgress = Get-ChildItem -LiteralPath $tempRoot -Filter "progress.json" `
        -File -Recurse -ErrorAction SilentlyContinue |
        Where-Object { $_.Directory.Name -match "^track-bucket-\d{4}$" } |
        Sort-Object -Property LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -ne $liveTrackProgress) {
        $trackProgress = Get-Content -LiteralPath $liveTrackProgress.FullName -Raw |
            ConvertFrom-Json
        $bucketNumber = [int]$trackProgress.source_bucket
        $bucketText = $bucketNumber.ToString("0000")
        $phase = "build_track_work_units / $($trackProgress.phase)"
        $unit = $bucketText
        if ($null -ne $trackProgress.current_shard) {
            $unit = "$bucketText / shard $($trackProgress.current_shard)"
        }
        $sourceFiles = @(Get-ChildItem -LiteralPath $partitionRoot `
            -Filter "*.parquet" -File -Recurse -ErrorAction SilentlyContinue |
            Where-Object { $_.FullName -match "mmsi_bucket=$bucketNumber([\\/])" })
        $sourceBytes = [int64](($sourceFiles | Measure-Object -Property Length -Sum).Sum)
        $activeOutputPath = $liveTrackProgress.Directory.FullName
    }
}

foreach ($month in $months) {
    if ($null -eq $phase -and -not (Test-ManifestItemComplete -Name "partition:$month")) {
        $phase = "partition_month"
        $unit = $month
        $sourceBytes = [int64](($splitRecords | Where-Object { $_.date.StartsWith($month) } |
            Measure-Object -Property dynamic_output_bytes -Sum).Sum)
        $year, $monthNumber = $month.Split("-")
        $activeOutputPath = Join-Path $workRoot "stage02_partitioned\year=$year\month=$monthNumber.tmp"
        break
    }
}

if ($null -eq $phase -and -not (Test-ManifestItemComplete -Name "static")) {
    $phase = "compact_static"
    $unit = "static"
    $sourceBytes = [int64](($splitRecords | Measure-Object -Property static_output_bytes -Sum).Sum)
    $activeOutputPath = Join-Path $workRoot "stage02_static\vessels.tmp.parquet"
}

if ($null -eq $phase) {
    for ($bucket = 0; $bucket -lt [int]$configInfo.mmsi_buckets; $bucket++) {
        $bucketText = $bucket.ToString("0000")
        if (-not (Test-ManifestItemComplete -Name "track-source:$bucketText")) {
            $phase = "build_track_work_units"
            $unit = $bucketText
            $sourceFiles = @(Get-ChildItem -LiteralPath (Join-Path $workRoot "stage02_partitioned") `
                -Filter "*.parquet" -File -Recurse -ErrorAction SilentlyContinue |
                Where-Object { $_.FullName -match "mmsi_bucket=$bucket([\\/])" })
            $sourceBytes = [int64](($sourceFiles | Measure-Object -Property Length -Sum).Sum)
            $activeOutputPath = Join-Path $tempRoot "track-bucket-$bucketText"
            break
        }
    }
}

if ($null -eq $phase) {
    throw "Prepare appears complete; no incomplete month, static table, or track source bucket was found."
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

function Get-PrepareProcessIds {
    $all = @(Get-CimInstance Win32_Process)
    $launchers = @($all | Where-Object {
        $_.Name -ieq "extractais.exe" -and $_.CommandLine -match "(?i)(^|\s)prepare(\s|$)"
    })
    if ($launchers.Count -eq 0) {
        $launchers = @($all | Where-Object {
            $_.Name -ieq "python.exe" -and
            $_.CommandLine -match "(?i)extractais" -and
            $_.CommandLine -match "(?i)(^|\s)prepare(\s|$)"
        })
    }
    if ($launchers.Count -eq 0) {
        return @()
    }

    $ids = [System.Collections.Generic.HashSet[int]]::new()
    foreach ($launcher in $launchers) {
        [void]$ids.Add([int]$launcher.ProcessId)
    }
    do {
        $added = $false
        foreach ($process in $all) {
            if ($ids.Contains([int]$process.ParentProcessId) -and -not $ids.Contains([int]$process.ProcessId)) {
                [void]$ids.Add([int]$process.ProcessId)
                $added = $true
            }
        }
    } while ($added)
    return @($ids)
}

$initialProcessIds = @(Get-PrepareProcessIds)
if ($initialProcessIds.Count -eq 0) {
    throw "No running 'extractais ... prepare' process tree was found."
}

$outputStart = Get-PathSnapshot -Path $activeOutputPath
$tempStart = Get-PathSnapshot -Path $tempRoot
$volumeStart = Get-Volume -DriveLetter $workDrive

Write-Host ""
Write-Host "ExtractAIS prepare diagnostic (read-only)"
Write-Host "Config             : $configPath"
Write-Host "Phase              : $phase"
Write-Host "Active unit        : $unit"
Write-Host ("Source size        : {0:N2} GiB" -f ($sourceBytes / 1GB))
Write-Host "Work root          : $workRoot"
Write-Host "DuckDB temp        : $tempRoot"
Write-Host "Physical disk      : $diskNumber / $($disk.FriendlyName) / $mediaType"
Write-Host "ExtractAIS / DuckDB: $($configInfo.extractais_version) / $($configInfo.duckdb_version)"
Write-Host "Threads            : $($configInfo.threads)"
Write-Host "Memory limit       : $($configInfo.memory_limit)"
Write-Host "MMSI buckets       : $($configInfo.mmsi_buckets)"
Write-Host "Open partition files: $($configInfo.partition_write_max_open_files)"
Write-Host "Reserve            : $($configInfo.minimum_free_space_gb) GiB"
Write-Host "Initial process IDs: $($initialProcessIds -join ', ')"
Write-Host ""
Write-Host "Time      CPU%  Cores  Private  Avail  ProcR  ProcW  DiskR  DiskW  Busy%  Queue  FreeTiB"

$sampleCount = [Math]::Max(1, [Math]::Ceiling($DurationMinutes * 60 / $IntervalSeconds))
$samples = [System.Collections.Generic.List[object]]::new()

for ($index = 0; $index -lt $sampleCount; $index++) {
    $processIds = @(Get-PrepareProcessIds)
    if ($processIds.Count -eq 0) {
        Write-Host "Prepare process ended during sampling."
        break
    }
    $processCounters = @(Get-CimInstance Win32_PerfFormattedData_PerfProc_Process |
        Where-Object { $processIds -contains [int]$_.IDProcess })
    if ($processCounters.Count -eq 0) {
        Write-Host "Prepare performance counters disappeared during sampling."
        break
    }
    $diskCounter = Get-CimInstance Win32_PerfFormattedData_PerfDisk_PhysicalDisk |
        Where-Object { $_.Name -match "^$diskNumber(\s|$)" } |
        Select-Object -First 1
    $memoryCounter = Get-CimInstance Win32_PerfFormattedData_PerfOS_Memory
    $volume = Get-Volume -DriveLetter $workDrive

    $cpuRaw = [double](($processCounters | Measure-Object -Property PercentProcessorTime -Sum).Sum)
    $privateBytes = [double](($processCounters | Measure-Object -Property WorkingSetPrivate -Sum).Sum)
    $processRead = [double](($processCounters | Measure-Object -Property IOReadBytesPersec -Sum).Sum)
    $processWrite = [double](($processCounters | Measure-Object -Property IOWriteBytesPersec -Sum).Sum)
    $sample = [pscustomobject]@{
        Timestamp = Get-Date
        ProcessCount = $processCounters.Count
        CpuPercent = if ($logicalProcessors) { $cpuRaw / $logicalProcessors } else { 0 }
        CpuCores = $cpuRaw / 100
        PrivateGiB = $privateBytes / 1GB
        AvailableMemoryGiB = [double]$memoryCounter.AvailableMBytes / 1024
        Handles = [double](($processCounters | Measure-Object -Property HandleCount -Sum).Sum)
        Threads = [double](($processCounters | Measure-Object -Property ThreadCount -Sum).Sum)
        ProcessReadMiBps = $processRead / 1MB
        ProcessWriteMiBps = $processWrite / 1MB
        DiskReadMiBps = [double]$diskCounter.DiskReadBytesPersec / 1MB
        DiskWriteMiBps = [double]$diskCounter.DiskWriteBytesPersec / 1MB
        DiskBusyPercent = [double]$diskCounter.PercentDiskTime
        DiskQueue = [double]$diskCounter.AvgDiskQueueLength
        FreeTiB = [double]$volume.SizeRemaining / 1TB
    }
    $samples.Add($sample)
    Write-Host ("{0:HH:mm:ss} {1,5:N1} {2,6:N2} {3,7:N1} {4,6:N1} {5,6:N1} {6,6:N1} {7,6:N1} {8,6:N1} {9,6:N0} {10,6:N1} {11,8:N2}" -f `
        $sample.Timestamp, $sample.CpuPercent, $sample.CpuCores, $sample.PrivateGiB, $sample.AvailableMemoryGiB,
        $sample.ProcessReadMiBps, $sample.ProcessWriteMiBps, $sample.DiskReadMiBps,
        $sample.DiskWriteMiBps, $sample.DiskBusyPercent, $sample.DiskQueue, $sample.FreeTiB)

    if ($index -lt $sampleCount - 1) {
        Start-Sleep -Seconds $IntervalSeconds
    }
}

if ($samples.Count -eq 0) {
    throw "No performance samples were collected before the prepare process ended."
}

$outputEnd = Get-PathSnapshot -Path $activeOutputPath
$tempEnd = Get-PathSnapshot -Path $tempRoot
$volumeEnd = Get-Volume -DriveLetter $workDrive

function Get-Average {
    param([string]$Property)
    return [double](($samples | Measure-Object -Property $Property -Average).Average)
}

function Get-Maximum {
    param([string]$Property)
    return [double](($samples | Measure-Object -Property $Property -Maximum).Maximum)
}

$summary = [pscustomobject]@{
    Samples = $samples.Count
    AverageCpuPercent = Get-Average "CpuPercent"
    AverageCpuCores = Get-Average "CpuCores"
    PeakPrivateGiB = Get-Maximum "PrivateGiB"
    MinimumAvailableMemoryGiB = [double](($samples | Measure-Object -Property AvailableMemoryGiB -Minimum).Minimum)
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

$findings = @(Get-PrepareDiagnosticFindings `
    -Summary $summary `
    -Phase $phase `
    -MmsiBuckets ([int]$configInfo.mmsi_buckets))

Write-Host "Findings"
foreach ($finding in $findings) {
    Write-Host "- $finding"
}
Write-Host ""
Write-Host "The script did not modify ExtractAIS data or process state."
