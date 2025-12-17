@echo off
setlocal

rem Change to repo directory
cd /d "%~dp0"

set VENV_DIR=.venv
set VENV_PY=%VENV_DIR%\Scripts\python.exe
set BASE_PY=

for %%P in (python.exe py.exe) do (
    for /f "delims=" %%I in ('where %%P 2^>nul') do (
        set BASE_PY=%%I
        goto :found_python
    )
)

echo Python 3.10+ is required but was not found in PATH. Install Python and try again.
pause
exit /b 1

:found_python
"%BASE_PY%" -c "import sys; sys.exit(0 if sys.version_info >= (3, 10) else 1)" || (
    echo Python 3.10+ is required. Found older version: & "%BASE_PY%" --version
    pause
    exit /b 1
)

if not exist "%VENV_PY%" (
    echo Creating virtual environment in %VENV_DIR% ...
    "%BASE_PY%" -m venv "%VENV_DIR%" || goto :error
) else (
    echo Using existing virtual environment in %VENV_DIR%.
)

echo Upgrading pip...
"%VENV_PY%" -m pip install --upgrade pip || goto :error

echo Installing LMU Telemetry dependencies...
"%VENV_PY%" -m pip install -r requirements.txt || goto :error

echo.
echo Dependencies installed successfully.
pause
exit /b 0

:error
echo.
echo Installation failed. Please check the messages above.
pause
exit /b 1

endlocal
