@echo off
setlocal
cd /d "%~dp0"

echo ======================================
echo VLDR - Inicio automatico al arrancar
echo ======================================

echo Creando tarea programada de usuario (sin admin)...
schtasks /Create /TN "VLDR_Server_AutoStart" /SC ONLOGON /TR "\"%~dp002_iniciar_servidor_red.bat\"" /RL LIMITED /F
if errorlevel 1 (
  echo ERROR: No se pudo crear la tarea.
  pause
  exit /b 1
)

echo.
echo Tarea creada: VLDR_Server_AutoStart
echo Para probar ahora: schtasks /Run /TN "VLDR_Server_AutoStart"
pause
