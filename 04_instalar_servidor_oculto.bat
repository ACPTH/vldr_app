@echo off
setlocal
cd /d "%~dp0"

echo ======================================
echo VLDR - Instalar servidor oculto

echo ======================================

if not exist ".venv\Scripts\python.exe" (
  echo ERROR: No existe .venv. Ejecuta 00_instalar_servidor.bat primero.
  pause
  exit /b 1
)

set "TASK=VLDR_Server_Hidden"
set "CMD=powershell.exe -NoProfile -ExecutionPolicy Bypass -WindowStyle Hidden -File \"%~dp0run_server_hidden.ps1\""

schtasks /Create /TN "%TASK%" /SC ONLOGON /TR "%CMD%" /RL LIMITED /F
if errorlevel 1 (
  echo ERROR: No se pudo crear la tarea.
  pause
  exit /b 1
)

echo.
echo Tarea creada: %TASK%
echo Iniciando ahora...
schtasks /Run /TN "%TASK%"

echo.
echo Listo. Queda ejecutandose en segundo plano al iniciar sesion.
pause
