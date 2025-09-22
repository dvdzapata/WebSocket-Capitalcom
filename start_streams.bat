@echo off
setlocal
cd /d %~dp0
set PYTHONIOENCODING=utf-8

start "CapitalStream" /B python streaming_capital.py
start "EODHDStream" /B python streaming_eodhd.py

REM Pequeña pausa para evitar que el programador termine antes de lanzar ambos procesos
ping 127.0.0.1 -n 3 >nul
exit /b 0
