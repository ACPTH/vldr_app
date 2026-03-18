@echo off
setlocal
cd /d "%~dp0"

echo ======================================
echo VLDR - Instalacion servidor local
echo ======================================

echo [1/4] Comprobando Python...
where py >nul 2>nul
if %errorlevel%==0 (
  set "PY_CMD=py"
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    set "PY_CMD=python"
  ) else (
    echo ERROR: No se encontro Python.
    echo Instala Python 3.11+ y marca "Add python.exe to PATH".
    pause
    exit /b 1
  )
)

echo [2/4] Creando entorno virtual (.venv)...
if not exist ".venv\Scripts\python.exe" (
  %PY_CMD% -m venv .venv
  if errorlevel 1 (
    echo ERROR: No se pudo crear el entorno virtual.
    pause
    exit /b 1
  )
) else (
  echo Entorno virtual ya existe.
)

echo [3/4] Activando entorno...
call ".venv\Scripts\activate.bat"
if errorlevel 1 (
  echo ERROR: No se pudo activar el entorno virtual.
  pause
  exit /b 1
)

echo [4/4] Instalando dependencias...
python -m pip install --upgrade pip
pip install -r requirements.txt
if errorlevel 1 (
  echo ERROR: Fallo la instalacion de dependencias.
  pause
  exit /b 1
)

echo.
echo Instalacion completada.
echo Siguiente paso: ejecutar 01_abrir_firewall_5050.bat y luego 02_iniciar_servidor_red.bat
pause
