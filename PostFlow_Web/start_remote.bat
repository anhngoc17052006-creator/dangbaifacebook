@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo Chua co moi truong Python. Hay chay start.bat mot lan truoc.
  pause
  exit /b 1
)
if not exist .postflow_token (
  .venv\Scripts\python.exe -c "import secrets; print(secrets.token_urlsafe(36))" > .postflow_token
)
set /p POSTFLOW_API_TOKEN=<.postflow_token
set POSTFLOW_ALLOWED_ORIGIN=https://anhngoc17052006-creator.github.io
set POSTFLOW_HOST=127.0.0.1
set POSTFLOW_PORT=8000
echo.
echo ================================================
echo PostFlow Backend da duoc bao ve.
echo Ma truy cap nam trong file .postflow_token
 echo Khong gui ma nay cho nguoi khac va khong tai len GitHub.
echo ================================================
echo.
.venv\Scripts\python.exe server.py
pause
