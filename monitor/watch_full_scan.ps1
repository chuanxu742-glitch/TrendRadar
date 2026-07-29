param(
    [string]$ContainerName = "trendradar-full-scan",
    [int]$PollSeconds = 30,
    [int]$MaxStallSeconds = 600,
    [int]$MaxRestarts = 5
)

$ErrorActionPreference = "Continue"
$repoRoot = Split-Path -Parent $PSScriptRoot
$composeFile = Join-Path $repoRoot "docker\docker-compose.yml"
$stateFile = Join-Path $repoRoot "output\official-monitor\status.json"
$progressFile = Join-Path $repoRoot "output\official-monitor\scan-progress.json"
$logFile = Join-Path $repoRoot "output\official-monitor\full-scan-supervisor.log"
$restartCount = 0

function Write-SupervisorLog([string]$Message) {
    $timestamp = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    # 日志超过 10MB 时轮转：当前日志改名为 .1 保留一份，旧 .1 被覆盖
    if (Test-Path -LiteralPath $logFile) {
        try {
            if ((Get-Item -LiteralPath $logFile).Length -gt 10MB) {
                Move-Item -LiteralPath $logFile -Destination "$logFile.1" -Force
            }
        }
        catch {
            # 轮转失败不阻塞监控主流程，继续追加写入
        }
    }
    Add-Content -LiteralPath $logFile -Value "[$timestamp] $Message" -Encoding UTF8
}

Write-SupervisorLog "watching container=$ContainerName"

while ($true) {
    $running = docker inspect $ContainerName --format "{{.State.Running}}" 2>$null
    if ($LASTEXITCODE -ne 0 -or $running -ne "true") {
        Write-SupervisorLog "full scan container stopped before completion; restoring normal monitor"
        docker compose -f $composeFile up -d official-monitor | Out-Null
        exit 1
    }

    if (Test-Path -LiteralPath $progressFile) {
        try {
            $progress = Get-Content -Raw -LiteralPath $progressFile | ConvertFrom-Json
            $lastProgress = [DateTimeOffset]::Parse([string]$progress.last_progress_at)
            $stallSeconds = ([DateTimeOffset]::Now - $lastProgress).TotalSeconds
            if ($stallSeconds -gt $MaxStallSeconds) {
                if ($restartCount -ge $MaxRestarts) {
                    Write-SupervisorLog "scan stalled for $([int]$stallSeconds)s and restart limit reached; restoring normal monitor"
                    docker stop $ContainerName | Out-Null
                    docker compose -f $composeFile up -d official-monitor | Out-Null
                    exit 3
                }
                $restartCount++
                Write-SupervisorLog "scan stalled for $([int]$stallSeconds)s; restarting container attempt=$restartCount"
                docker restart $ContainerName | Out-Null
                Start-Sleep -Seconds 10
                continue
            }
        }
        catch {
            Write-SupervisorLog "unable to read progress heartbeat: $($_.Exception.Message)"
        }
    }

    $recentLogs = docker logs --tail 30 $ContainerName 2>&1
    if ($recentLogs -match "\[official-monitor\] scan complete;") {
        $status = $null
        try {
            $status = Get-Content -Raw -LiteralPath $stateFile | ConvertFrom-Json
        }
        catch {
            Write-SupervisorLog "unable to read final status: $($_.Exception.Message)"
        }

        $success = $null -ne $status `
            -and [int]$status.cycle.pending -eq 0 `
            -and [int]$status.cycle.batch_size -ge [int]$status.inventory.unique_sources

        docker stop $ContainerName | Out-Null
        docker compose -f $composeFile up -d official-monitor | Out-Null

        if ($success) {
            docker compose -f $composeFile restart trendradar | Out-Null
            Write-SupervisorLog "full scan complete sources=$($status.inventory.unique_sources); normal monitor restored; TrendRadar restarted"
            exit 0
        }

        Write-SupervisorLog "full scan ended without complete coverage; normal monitor restored"
        exit 2
    }

    Start-Sleep -Seconds $PollSeconds
}
