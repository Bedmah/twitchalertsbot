param(
    [ValidateSet("start", "stop", "restart", "status", "logs")]
    [string]$Action = "status",
    [int]$Tail = 60
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$BotScript = Join-Path $ScriptDir "bot.py"
$PidFile = Join-Path $ScriptDir "bot.pid"
$StdOutLog = Join-Path $ScriptDir "bot_runtime.log"
$StdErrLog = Join-Path $ScriptDir "bot_runtime.err.log"

function Get-BotProcess {
    if (Test-Path $PidFile) {
        $raw = (Get-Content -Raw $PidFile).Trim()
        if ($raw -match "^\d+$") {
            $knownPid = [int]$raw
            $meta = Get-CimInstance Win32_Process -Filter "ProcessId = $knownPid" -ErrorAction SilentlyContinue
            if ($meta -and $meta.Name -eq "python.exe" -and $meta.CommandLine -match "bot\.py") {
                return Get-Process -Id $knownPid -ErrorAction SilentlyContinue
            }
        }
    }

    $proc = Get-CimInstance Win32_Process |
        Where-Object { $_.Name -eq "python.exe" -and $_.CommandLine -match "bot\.py" } |
        Select-Object -First 1

    if ($proc) {
        return Get-Process -Id $proc.ProcessId -ErrorAction SilentlyContinue
    }

    return $null
}

function Start-Bot {
    $existing = Get-BotProcess
    if ($existing) {
        Write-Output "already_running pid=$($existing.Id)"
        return
    }

    if (Test-Path $StdOutLog) { Remove-Item $StdOutLog -Force }
    if (Test-Path $StdErrLog) { Remove-Item $StdErrLog -Force }

    $proc = Start-Process -FilePath "python" `
        -ArgumentList "bot.py" `
        -WorkingDirectory $ScriptDir `
        -RedirectStandardOutput $StdOutLog `
        -RedirectStandardError $StdErrLog `
        -PassThru

    Start-Sleep -Seconds 2
    $check = Get-Process -Id $proc.Id -ErrorAction SilentlyContinue
    if ($check) {
        Set-Content -Path $PidFile -Value $proc.Id -Encoding ascii
        Write-Output "started pid=$($proc.Id)"
        return
    }

    Write-Output "failed_to_start"
    if (Test-Path $StdErrLog) {
        Write-Output "--- stderr ---"
        Get-Content $StdErrLog
    }
}

function Stop-Bot {
    $existing = Get-BotProcess
    if (-not $existing) {
        if (Test-Path $PidFile) { Remove-Item $PidFile -Force }
        Write-Output "not_running"
        return
    }

    Stop-Process -Id $existing.Id -Force
    Start-Sleep -Milliseconds 400
    if (Test-Path $PidFile) { Remove-Item $PidFile -Force }
    Write-Output "stopped pid=$($existing.Id)"
}

function Show-Status {
    $existing = Get-BotProcess
    if ($existing) {
        Write-Output "running pid=$($existing.Id)"
    }
    else {
        Write-Output "stopped"
    }
}

function Show-Logs {
    if (Test-Path $StdOutLog) {
        Write-Output "--- stdout (last $Tail) ---"
        Get-Content $StdOutLog -Tail $Tail
    }
    else {
        Write-Output "stdout_log_not_found"
    }

    if (Test-Path $StdErrLog) {
        Write-Output "--- stderr (last $Tail) ---"
        Get-Content $StdErrLog -Tail $Tail
    }
    else {
        Write-Output "stderr_log_not_found"
    }
}

switch ($Action) {
    "start" { Start-Bot }
    "stop" { Stop-Bot }
    "restart" { Stop-Bot; Start-Bot }
    "status" { Show-Status }
    "logs" { Show-Logs }
    default { Show-Status }
}
