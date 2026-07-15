<# Windows management wrapper for the 1C -> Kafka -> PostgreSQL contour. #>
param(
    [Parameter(Position = 0)]
    [ValidateSet('up','down','restart','build','logs','ps','topics','psql',
                 'sync-full','sync-incremental','demo-touch','demo-delete',
                 'verify','health','onec-check','test','test-integration',
                 'clean','reset','help')]
    [string]$Command = 'help',

    [string]$Id,
    [string]$Name
)

$ErrorActionPreference = 'Stop'
$OutputEncoding = New-Object System.Text.UTF8Encoding($false)
[Console]::OutputEncoding = $OutputEncoding

function Invoke-Native {
    param(
        [Parameter(Mandatory = $true)][string]$FilePath,
        [Parameter(Mandatory = $true)][string[]]$ArgumentList
    )
    & $FilePath @ArgumentList
    if ($LASTEXITCODE -ne 0) {
        throw "$FilePath failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Compose {
    param([Parameter(Mandatory = $true)][string[]]$ArgumentList)
    Invoke-Native -FilePath 'docker' -ArgumentList (@('compose') + $ArgumentList)
}

function Invoke-TestCompose {
    param([Parameter(Mandatory = $true)][string[]]$ArgumentList)
    Invoke-Native -FilePath 'docker' -ArgumentList (
        @('compose', '-f', 'compose.yaml', '-f', 'compose.test.yaml') + $ArgumentList
    )
}

switch ($Command) {
    'up' {
        Invoke-Compose @('up', '-d', '--build')
        Write-Host 'Done. Kafka UI: http://localhost:8080'
    }
    'down'      { Invoke-Compose @('down') }
    'restart'   { Invoke-Compose @('down'); Invoke-Compose @('up', '-d', '--build') }
    'build'     { Invoke-Compose @('build') }
    'logs'      { Invoke-Compose @('logs', '-f') }
    'ps'        { Invoke-Compose @('ps') }
    'topics'    { Invoke-Compose @('exec', 'kafka', '/opt/kafka/bin/kafka-topics.sh', '--bootstrap-server', 'kafka:19092', '--list') }
    'psql'      { Invoke-Compose @('exec', 'postgres', 'sh', '-lc', 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB"') }
    'sync-full' { Invoke-Compose @('exec', 'integration-service', 'python', '-m', 'integration', 'sync', 'full') }
    'sync-incremental' { Invoke-Compose @('exec', 'integration-service', 'python', '-m', 'integration', 'sync', 'incremental') }
    'demo-touch' {
        if (-not $Id) { throw 'Specify -Id <guid>' }
        $arguments = @('exec', 'integration-service', 'python', '-m', 'integration', 'demo', 'touch', $Id)
        if ($Name) { $arguments += @('--name', $Name) }
        Invoke-Compose $arguments
    }
    'demo-delete' {
        if (-not $Id) { throw 'Specify -Id <guid>' }
        Invoke-Compose @('exec', 'integration-service', 'python', '-m', 'integration', 'demo', 'delete', $Id)
    }
    'verify' {
        Invoke-Compose @('--progress', 'quiet', 'cp', 'sql/verify.sql', 'postgres:/tmp/verify.sql')
        Invoke-Compose @('exec', '-T', 'postgres', 'sh', '-lc', 'psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" -f /tmp/verify.sql')
    }
    'health' {
        $response = Invoke-WebRequest -Uri 'http://localhost:8081/health' -UseBasicParsing
        $response.Content
    }
    'onec-check' {
        $code = "import os,httpx; u=os.environ['ONEC_BASE_URL']; r=httpx.get(u+'/ownership-forms',timeout=30); r.raise_for_status(); print('URL:',u); print('HTTP',r.status_code); print(r.text[:200])"
        Invoke-Compose @('exec', 'integration-service', 'python', '-c', $code)
    }
    'test' {
        Invoke-Native -FilePath 'uv' -ArgumentList @('run', '--directory', 'integration-service', 'pytest', '-q', '-m', 'not integration')
        Invoke-Native -FilePath 'uv' -ArgumentList @('run', '--directory', 'consumer-service', 'pytest', '-q', '-m', 'not postgres')
    }
    'test-integration' {
        Invoke-TestCompose @('run', '--build', '--rm', '--no-deps', '--entrypoint', 'python', 'integration-service', '-m', 'pytest', '-q', '-m', 'integration')
    }
    'clean'   { Invoke-Compose @('down', '--remove-orphans') }
    'reset'   { Invoke-Compose @('down', '-v', '--remove-orphans') }
    default {
        Write-Host 'Commands: up, down, restart, build, logs, ps, topics, psql,'
        Write-Host '          sync-full, sync-incremental, demo-touch, demo-delete,'
        Write-Host '          verify, health, onec-check, test, test-integration,'
        Write-Host '          clean, reset'
    }
}
