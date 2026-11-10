# Runs a load script inside the compose network (no Windows <-> Docker Desktop port forwarding).
# Usage: .\load\run_in_network.ps1 loadtest.py --runs 50 --rate 10
# $env:GEN_CPUS = "1" caps the generator's CPU (real clients run on other machines; uncapped, the
# generator competes with the API and Postgres for the same 4 cores).
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$ScriptArgs)
$root = Split-Path -Parent $PSScriptRoot
$cpu = @()
if ($env:GEN_CPUS) { $cpu = @("--cpus", $env:GEN_CPUS) }
docker run --rm @cpu --network taskforwork_default -v "${root}:/w" -w /w `
  -e API_URL=http://api:8000 -e DB_OWNER_URL=postgresql://owner:owner@db:5432/tracker `
  python:3.11-slim sh -c "pip install -q --root-user-action=ignore -r load/requirements.txt 2>/dev/null; python load/$($ScriptArgs -join ' ')"
