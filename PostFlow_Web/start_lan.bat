@echo off
chcp 65001 >nul
cd /d "%~dp0"
if not exist .venv\Scripts\python.exe (echo Hay chay start.bat mot lan truoc de cai dat.& pause & exit /b 1)
echo CANH BAO: Che do LAN cho phep thiet bi cung mang dieu khien phien Facebook tren may nay.
echo Chi dung tren mang Wi-Fi gia dinh/van phong tin cay. Khong port-forward ra Internet.
set POSTFLOW_HOST=0.0.0.0
set POSTFLOW_PORT=8000
.venv\Scripts\python.exe server.py
pause
