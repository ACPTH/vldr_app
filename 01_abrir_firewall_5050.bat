@echo off
setlocal

echo ======================================
echo VLDR - Abrir puerto 5050 en firewall
echo ======================================
echo.
echo Este script requiere PowerShell/CMD como administrador.

echo Creando regla de entrada TCP 5050...
netsh advfirewall firewall add rule name="VLDR 5050 TCP" dir=in action=allow protocol=TCP localport=5050
if errorlevel 1 (
  echo.
  echo ERROR: No se pudo crear la regla.
  echo Ejecuta este .bat como ADMINISTRADOR.
  pause
  exit /b 1
)

echo.
echo Regla creada correctamente.
pause
