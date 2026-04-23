@echo off
REM Startup script for Windows (cmd.exe).
REM Creates a local virtualenv on first run, installs dependencies, and
REM launches zerobus_feeder.py forwarding any arguments you pass.
REM
REM Usage:
REM   run.bat                         # interactive first-run wizard
REM   run.bat --config config.yaml    # run with a YAML config
REM   run.bat --non-interactive       # reuse last-saved values

setlocal enableextensions
cd /d "%~dp0"

set "VENV_DIR=.venv"
set "STAMP=%VENV_DIR%\requirements.stamp"

set "PYTHON_BIN=%PYTHON%"
if "%PYTHON_BIN%"=="" (
    where py >nul 2>nul
    if not errorlevel 1 (
        set "PYTHON_BIN=py -3"
    ) else (
        where python >nul 2>nul
        if not errorlevel 1 (
            set "PYTHON_BIN=python"
        )
    )
)

if "%PYTHON_BIN%"=="" (
    echo Error: no Python interpreter found on PATH. Install Python 3.9+ from
    echo https://www.python.org/downloads/ and re-run.
    exit /b 1
)

%PYTHON_BIN% -c "import sys; raise SystemExit(0 if sys.version_info >= (3, 9) else 1)" >nul 2>nul
if errorlevel 1 (
    echo Error: Python 3.9+ is required.
    exit /b 1
)

if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo Creating virtualenv in %VENV_DIR% ...
    %PYTHON_BIN% -m venv "%VENV_DIR%"
    if errorlevel 1 exit /b 1
)

call "%VENV_DIR%\Scripts\activate.bat"

set "NEEDS_INSTALL=0"
if not exist "%STAMP%" set "NEEDS_INSTALL=1"
if "%NEEDS_INSTALL%"=="0" (
    for %%I in (requirements.txt) do set "REQ_TIME=%%~tI"
    for %%I in (%STAMP%) do set "STAMP_TIME=%%~tI"
    REM Reinstall if requirements.txt is newer than the stamp.
    if "%REQ_TIME%" GTR "%STAMP_TIME%" set "NEEDS_INSTALL=1"
)

if "%NEEDS_INSTALL%"=="1" (
    echo Installing dependencies from requirements.txt ...
    python -m pip install --upgrade pip >nul
    python -m pip install -r requirements.txt
    if errorlevel 1 exit /b 1
    echo done > "%STAMP%"
)

python zerobus_feeder.py %*
exit /b %errorlevel%
