@echo off
setlocal

rem Ensure we run from the repo folder
cd /d "%~dp0"

set VENV_PY=.venv\Scripts\python.exe
set PY_CMD=python

if exist "%VENV_PY%" (
    set PY_CMD=%VENV_PY%
)

%PY_CMD% telemetry_gui_oneclick.py
if errorlevel 1 (
    echo.
    echo ERROR: Python was not found or dependencies are missing.
    echo Install Python 3.10+ and run install_dependencies.bat to set up requirements.
    pause
)

endlocal
