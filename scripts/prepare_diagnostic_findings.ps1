function Get-PrepareDiagnosticFindings {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true)]
        [psobject]$Summary,

        [Parameter(Mandatory = $true)]
        [string]$Phase,

        [Parameter(Mandatory = $true)]
        [int]$MmsiBuckets
    )

    $findings = [System.Collections.Generic.List[string]]::new()
    if ($Summary.AverageCpuPercent -ge 70) {
        $findings.Add("CPU-bound: compression or query execution keeps most logical CPUs busy.")
    }
    if ($Summary.AverageDiskBusyPercent -ge 80 -and $Summary.AverageDiskQueue -ge 2) {
        if (($Summary.AverageDiskReadMiBps + $Summary.AverageDiskWriteMiBps) -lt 30) {
            $findings.Add("Storage-bound with low throughput: the disk is busy but transfers little data, consistent with random I/O or fragmented partition writes.")
        } else {
            $findings.Add("Storage-bound: the work disk is saturated and has a sustained queue.")
        }
    }
    if ($Summary.TempGrowthGiB -gt 1) {
        $findings.Add("DuckDB spill detected: temporary files grew by more than 1 GiB during the sample.")
    }
    if ($Summary.MinimumAvailableMemoryGiB -lt 10) {
        $findings.Add("Memory pressure detected: system available memory fell below 10 GiB.")
    }
    $isPartitionPhase = $Phase -eq "partition_month" -or $Phase -eq "prepare"
    if ($isPartitionPhase -and $Summary.OutputFilesAtEnd -gt $MmsiBuckets) {
        $findings.Add("Partition file multiplication detected: output file count exceeds the configured bucket count.")
    }
    if ($isPartitionPhase -and
        $Summary.AverageCpuCores -lt 2 -and
        $Summary.OutputFilesAtEnd -ge $MmsiBuckets -and
        $Summary.PeakPrivateGiB -ge 32 -and
        $Summary.AverageProcessWriteMiBps -ge 30) {
        $findings.Add("Partition writer fan-out detected: many bucket files are open with high buffering while execution remains near one CPU core.")
    }
    if ($Summary.AverageCpuPercent -lt 30 -and $Summary.AverageDiskBusyPercent -lt 60 -and
        ($Summary.AverageProcessReadMiBps + $Summary.AverageProcessWriteMiBps) -lt 30) {
        $findings.Add("Low resource utilization: inspect antivirus/indexing, file-system metadata overhead, or a query phase with limited parallelism.")
    }
    if ($findings.Count -eq 0) {
        $findings.Add("No single dominant bottleneck was identified; use the numeric summary for comparison across a longer sample.")
    }
    return $findings
}
