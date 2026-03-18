$ErrorActionPreference = 'Stop'
$base = Split-Path -Parent $MyInvocation.MyCommand.Path
Set-Location $base

$py = Join-Path $base '.venv\Scripts\python.exe'
if (-not (Test-Path $py)) {
  throw 'No existe .venv. Ejecuta 00_instalar_servidor.bat primero.'
}

$env:PORT = '5050'
$env:PYTHONUTF8 = '1'
$env:NO_BROWSER = '1'

& $py 'app.py'
