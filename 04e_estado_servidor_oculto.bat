@echo off
setlocal
echo ===== Estado tarea =====
schtasks /Query /TN "VLDR_Server_Hidden"
echo.
echo ===== Procesos python =====
tasklist /FI "IMAGENAME eq python.exe"
pause
