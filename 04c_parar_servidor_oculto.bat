@echo off
setlocal
powershell -NoProfile -Command "Get-CimInstance Win32_Process | Where-Object { $_.Name -eq 'python.exe' -and $_.CommandLine -match 'vldr_app\\app.py' } | ForEach-Object { Stop-Process -Id $_.ProcessId -Force }"
schtasks /End /TN "VLDR_Server_Hidden" >nul 2>nul
echo Servidor oculto detenido (si estaba en ejecucion).
pause
