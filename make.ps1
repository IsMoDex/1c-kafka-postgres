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
                 'verify','health','onec-check','test','clean','reset','help')]
    [string]$Command = 'help',

    [string]$Id,
    [string]$Name
)

$ErrorActionPreference = 'Stop'
$OutputEncoding = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $OutputEncoding

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
    'psql'      { Invoke-Compose "exec postgres sh -lc 'psql -U `"`$POSTGRES_USER`" -d `"`$POSTGRES_DB`"'" }
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
        & docker compose cp 'sql/verify.sql' 'postgres:/tmp/verify.sql'
        & docker compose exec -T postgres sh -lc 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/verify.sql'
    }
    'health'  { try { (Invoke-WebRequest -Uri 'http://localhost:8081/health' -UseBasicParsing).Content } catch { 'consumer unavailable' } }
    'onec-check' { Invoke-Compose "exec integration-service python -c ""import os,httpx; u=os.environ['ONEC_BASE_URL']; r=httpx.get(u+'/ownership-forms',timeout=30); print('URL:',u); print('HTTP',r.status_code); print(r.text[:200])""" }
    'test' {
        Invoke-Compose 'exec integration-service python -m pytest -q'
        Invoke-Compose 'exec consumer-service python -m pytest -q'
    }
    'clean'   { Invoke-Compose 'down --remove-orphans' }
    'reset'   { Invoke-Compose 'down -v --remove-orphans' }
    default {
        Write-Host 'Commands: up, down, restart, build, logs, ps, topics, psql,'
        Write-Host '          sync-full, sync-incremental, demo-touch, demo-delete,'
        Write-Host '          verify, health, onec-check, test, clean, reset'
    }
}
