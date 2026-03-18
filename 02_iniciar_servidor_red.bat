@echo off
setlocal
cd /d "%~dp0"

echo ======================================
echo VLDR - Servidor en red local
echo ======================================

if not exist ".venv\Scripts\python.exe" (
  echo No existe .venv. Ejecuta primero 00_instalar_servidor.bat
  pause
  exit /b 1
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 (
  echo ERROR: No se pudo activar el entorno virtual.
  pause
  exit /b 1
)

set "PORT=5050"
set "PYTHONUTF8=1"

echo Iniciando servidor...
echo URL local: http://127.0.0.1:5050
echo URL red  : http://%COMPUTERNAME%:5050
echo.
echo Deja esta ventana abierta para mantener el servidor activo.
echo.
python app.py

echo.
echo El servidor se ha detenido.
pause
