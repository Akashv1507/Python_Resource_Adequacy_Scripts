@echo off
REM --- Define the format for the date-time variables ---
REM The required format is YYYY-MM-DD HH:MM:SS, but the executable might
REM accept YYYYMMDDHHMMSS or similar. Assuming YYYY-MM-DD for this script.

REM --- 1. Calculate Yesterday's Date (YYYY-MM-DD) using PowerShell ---
for /f "usebackq" %%i in (`powershell -Command "(Get-Date).AddDays(-1).ToString('yyyy-MM-dd')"`) do set "YESTERDAY_DATE=%%i"

REM --- 2. Construct START_DTTime and END_DTTime variables ---
REM Start time: Yesterday's Date at 00:00:00
set "START_DTTime=%YESTERDAY_DATE% 00:00:00"

REM End time: Yesterday's Date at 23:59:00
set "END_DTTime=%YESTERDAY_DATE% 23:59:00"

REM --- 3. Display the generated variables for verification ---
echo.
echo Yesterday's Date: %YESTERDAY_DATE%
echo START_DTTime: %START_DTTime%
echo END_DTTime: %END_DTTime%
echo.

REM --- 4. Execute the command with the calculated dates ---
echo Running executable...
index_pushOutageSummaryData.exe --start_dateTime "%START_DTTime%" --end_dateTime "%END_DTTime%"

echo.
echo Script execution complete.
