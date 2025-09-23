@echo off
setlocal
cd /d %~dp0
set PYTHONIOENCODING=utf-8

python streaming_combined.py

exit /b %errorlevel%
