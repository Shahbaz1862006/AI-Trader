@echo off
chcp 65001 >nul

echo.
echo ╔══════════════════════════════════════════════╗
echo ║     AI TRADING SYSTEM - SIMULATION MODE      ║
echo ╚══════════════════════════════════════════════╝
echo.

:: Check .env file exists
if not exist ".env" (
    echo ERROR: .env file not found!
    echo.
    echo Create a .env file in this folder with your API keys.
    echo See README.md for instructions.
    echo.
    pause
    exit /b 1
)

:: Check Python installed
python --version >nul 2>&1
if errorlevel 1 (
    echo ERROR: Python not found!
    echo Please install Python from https://python.org
    echo Make sure to check "Add Python to PATH" during install
    pause
    exit /b 1
)

echo Installing dependencies...
pip install -r requirements.txt -q

echo.
echo Starting API Server on port 5000...
start "API Server" cmd /k "cd backend && python api_server.py"

timeout /t 2 /nobreak >nul

echo Starting Trading Bot...
start "Trading Bot" cmd /k "cd backend && python trading_bot.py"

echo.
echo ┌─────────────────────────────────────────────┐
echo │  SYSTEM RUNNING!                            │
echo │                                             │
echo │  Two new windows have opened:               │
echo │  1. API Server  (port 5000)                 │
echo │  2. Trading Bot (main engine)               │
echo │                                             │
echo │  Now open: frontend\index.html              │
echo │  in your browser to see the dashboard       │
echo │                                             │
echo │  Close both windows to stop the bot         │
echo └─────────────────────────────────────────────┘
echo.
pause
