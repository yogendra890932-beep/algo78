@echo off
REM run.bat — Double-click this file to start the trading bot server on Windows

echo ============================================
echo   Optiscalper Trading Bot
echo ============================================
echo.

REM Check if venv exists
if not exist "venv\Scripts\activate.bat" (
    echo ERROR: Virtual environment not found.
    echo Please run these commands first:
    echo   python -m venv venv
    echo   venv\Scripts\activate
    echo   pip install -r requirements.txt
    echo   python scripts\init_db.py
    pause
    exit /b 1
)

REM Activate virtual environment
call venv\Scripts\activate.bat

REM Check if .env exists
if not exist ".env" (
    echo ERROR: .env file not found.
    echo Please copy .env.example to .env and fill in your keys.
    pause
    exit /b 1
)

echo Starting server...
echo Open http://localhost:8000 in your browser
echo Press Ctrl+C to stop
echo.

uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload

pause
