<#
.SYNOPSIS
  Management wrapper for the 1C -> Kafka -> PostgreSQL integration contour on
  Windows (analog of the Makefile for environments without GNU make).

.EXAMPLE
  .\make.ps1 up
  .\make.ps1 sync-full
  .\make.ps1 sync-incremental
  .\make.ps1 demo-touch -Id b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0001 -Name "New name"
  .\make.ps1 demo-delete -Id b7e2a1f0-3b5d-4a1d-8d5a-1d6c8c1a0005
  .\make.ps1 verify
  .\make.ps1 down
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet('up','down','restart','build','logs','ps','topics','psql',
                 'sync-full','sync-incremental','demo-touch','demo-delete',
                 'verify','health','clean','reset','help')]
    [string]$Command = 'help',

    [string]$Id,
    [string]$Name
)

$ErrorActionPreference = 'Stop'

function Invoke-Compose { param([string]$ComposeArgs) Invoke-Expression "docker compose $ComposeArgs" }

switch ($Command) {
    'up' {
        Invoke-Compose 'up -d --build'
        Write-Host 'Done. Kafka UI: http://localhost:8080'
    }
    'down'      { Invoke-Compose 'down' }
    'restart'   { Invoke-Compose 'down'; Invoke-Compose 'up -d --build' }
    'build'     { Invoke-Compose 'build' }
    'logs'      { Invoke-Compose 'logs -f' }
    'ps'        { Invoke-Compose 'ps' }
    'topics'    { Invoke-Compose 'exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka:19092 --list' }
    'psql'      { Invoke-Compose 'exec postgres psql -U integration -d integration' }
    'sync-full' { Invoke-Compose 'exec integration-service python -m integration sync full' }
    'sync-incremental' { Invoke-Compose 'exec integration-service python -m integration sync incremental' }
    'demo-touch' {
        if (-not $Id) { throw 'Specify -Id <guid>' }
        $nameArg = if ($Name) { "--name `"$Name`"" } else { '' }
        Invoke-Compose "exec integration-service python -m integration demo touch $Id $nameArg"
    }
    'demo-delete' {
        if (-not $Id) { throw 'Specify -Id <guid>' }
        Invoke-Compose "exec integration-service python -m integration demo delete $Id"
    }
    'verify' {
        Get-Content -Raw -Encoding UTF8 'sql/verify.sql' |
            & docker compose exec -T postgres psql -U integration -d integration -f -
    }
    'health'  { try { (Invoke-WebRequest -Uri 'http://localhost:8081/health' -UseBasicParsing).Content } catch { 'consumer unavailable' } }
    'clean'   { Invoke-Compose 'down --remove-orphans' }
    'reset'   { Invoke-Compose 'down -v --remove-orphans' }
    default {
        Write-Host 'Commands: up, down, restart, build, logs, ps, topics, psql,'
        Write-Host '          sync-full, sync-incremental, demo-touch, demo-delete,'
        Write-Host '          verify, health, clean, reset'
    }
}
