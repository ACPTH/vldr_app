@echo off
setlocal
schtasks /Run /TN "VLDR_Server_Hidden"
if errorlevel 1 (
  echo ERROR al iniciar tarea VLDR_Server_Hidden.
) else (
  echo Tarea iniciada.
)
pause
