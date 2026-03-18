@echo off
setlocal
echo Eliminando tarea VLDR_Server_AutoStart...
schtasks /Delete /TN "VLDR_Server_AutoStart" /F
pause
