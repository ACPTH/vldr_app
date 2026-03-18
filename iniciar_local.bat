@echo off
setlocal
cd /d "%~dp0"

echo ======================================
echo   VLDR - Inicio local

echo ======================================

if exist ".venv\Scripts\python.exe" goto VENV_OK

echo [1/4] Creando entorno virtual...
where py >nul 2>nul
if %errorlevel%==0 (
  py -m venv .venv
) else (
  where python >nul 2>nul
  if %errorlevel%==0 (
    python -m venv .venv
  ) else (
    echo ERROR: No se encontro Python.
    echo Instala Python 3 y marca "Add python.exe to PATH".
    pause
    exit /b 1
  )
)

:VENV_OK
echo [2/4] Activando entorno...
call ".venv\Scripts\activate.bat"
if errorlevel 1 (
  echo ERROR: No se pudo activar el entorno virtual.
  pause
  exit /b 1
)

echo [3/4] Instalando/actualizando dependencias...
python -m pip install --upgrade pip
pip install -r requirements.txt
if errorlevel 1 (
  echo ERROR: Fallo la instalacion de dependencias.
  pause
  exit /b 1
)

echo [4/4] Iniciando servidor...
echo Abre: http://127.0.0.1:5050
python app.py

echo.
echo El servidor se ha detenido.
pause
