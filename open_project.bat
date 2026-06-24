@echo off
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
  echo Node.js is not installed or not available in PATH.
  pause
  exit /b 1
)

start "FAIR prototype server" /min cmd /k "npm.cmd start"

echo Starting server...
for /l %%i in (1,1,15) do (
  powershell -NoProfile -ExecutionPolicy Bypass -Command "try { $r = Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:3001/index.html' -TimeoutSec 1; if ($r.StatusCode -eq 200) { exit 0 } } catch { exit 1 }"
  if not errorlevel 1 (
    start "" "http://127.0.0.1:3001/index.html"
    exit /b 0
  )
  timeout /t 1 /nobreak >nul
)

echo Server did not start on http://127.0.0.1:3001/index.html
echo Check the server window for errors.
pause
exit /b
