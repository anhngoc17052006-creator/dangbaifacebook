@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (
  echo [1/3] Tao moi truong Python...
  py -3 -m venv .venv
  if errorlevel 1 (echo Khong tao duoc moi truong. Hay cai Python 3.10+ va thu lai.& pause & exit /b 1)
)
echo [2/3] Cai dat thu vien...
.venv\Scripts\python.exe -m pip install --upgrade pip
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 (echo Cai thu vien that bai.& pause & exit /b 1)
echo [3/3] Cai Chromium cho Playwright...
.venv\Scripts\python.exe -m playwright install chromium
if errorlevel 1 (echo Cai Chromium that bai.& pause & exit /b 1)
echo.
echo PostFlow se mo tai http://127.0.0.1:8000
echo API token duoc tao tu dong tai: %CD%\.postflow_token
echo Khong chia se file token va khong tai file nay len GitHub.
start "PostFlow Web" http://127.0.0.1:8000
.venv\Scripts\python.exe server.py
pause
