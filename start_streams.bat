@echo off
setlocal
cd /d %~dp0
set PYTHONIOENCODING=utf-8

echo Iniciando streaming_combined.py (reinicio automático en caso de salida)...
:loop
python -u streaming_combined.py
set EXITCODE=%ERRORLEVEL%
echo [%date% %time%] streaming_combined.py finalizó con código %EXITCODE%. Reinicio en 5 segundos...
timeout /t 5 /nobreak >nul
goto loop
