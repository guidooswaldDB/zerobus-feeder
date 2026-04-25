@echo off
REM Removes locally stored connection info and logs so the next run starts clean.
REM Targets:
REM   .zerobus_feeder_last.yaml   remembered values + client secret
REM   feeder_config.yaml          wizard-generated config + client secret
REM   zerobus_feeder.log          session log
REM
REM Usage:
REM   cleanup.bat

setlocal enableextensions
cd /d "%~dp0"

set "REMOVED=0"

for %%F in (".zerobus_feeder_last.yaml" "feeder_config.yaml" "zerobus_feeder.log") do (
    if exist %%F (
        del /q %%F
        echo removed %%~F
        set /a REMOVED+=1
    )
)

if "%REMOVED%"=="0" echo nothing to clean

endlocal
