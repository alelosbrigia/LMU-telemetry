@echo off
setlocal

REM Vai nella cartella dove si trova questo .bat
cd /d "%~dp0"

REM Avvia la GUI con Python
python telemetry_gui_oneclick.py

REM Se Python non è nel PATH, mostra un messaggio
if errorlevel 1 (
    echo.
    echo ERRORE: Python non trovato.
    echo Assicurati che Python 3.10+ sia installato e nel PATH.
    pause
)

endlocal