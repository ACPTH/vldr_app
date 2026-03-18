@echo off
setlocal
schtasks /Delete /TN "VLDR_Server_Hidden" /F
if errorlevel 1 (
  echo ERROR al eliminar tarea.
) else (
  echo Tarea eliminada.
)
pause
