Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

$root = Split-Path -Parent $MyInvocation.MyCommand.Path

Push-Location $root
try
{
    docker compose down -v --remove-orphans
}
finally
{
    Pop-Location
}
