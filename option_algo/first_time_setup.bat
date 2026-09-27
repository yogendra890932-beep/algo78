@echo off
REM first_time_setup.bat — Run ONCE after downloading
REM Double-click this file to set up everything automatically

echo ============================================
echo   Optiscalper — First Time Setup
echo ============================================
echo.

REM ── Check Python ──────────────────────────────
echo [1/5] Checking Python...
python --version >nul 2>&1
if errorlevel 1 (
    echo.
    echo ERROR: Python not found!
    echo.
    echo Please:
    echo   1. Go to https://www.python.org/downloads/
    echo   2. Download Python 3.11
    echo   3. CHECK "Add Python to PATH" during install
    echo   4. Run this file again
    pause
    exit /b 1
)
python --version
echo.

REM ── Virtual environment ───────────────────────
echo [2/5] Creating virtual environment...
if exist "venv\Scripts\activate.bat" (
    echo Already exists, skipping.
) else (
    python -m venv venv
    echo Created.
)
echo.
call venv\Scripts\activate.bat

REM ── Install packages ──────────────────────────
echo [3/5] Installing packages (takes 3-5 min)...
pip install -r requirements.txt --quiet
if errorlevel 1 (
    echo.
    echo ERROR: Package install failed.
    echo Try: pip install -r requirements.txt
    pause
    exit /b 1
)
echo Packages installed.
echo.

REM ── .env setup ────────────────────────────────
echo [4/5] Setting up .env file...
if exist ".env" (
    echo .env exists, skipping.
) else (
    copy .env.example .env >nul
    echo.
    echo =============================================
    echo  COPY these keys into your .env file:
    echo =============================================
    echo.
    echo SECRET_KEY:
    python -c "import secrets; print(secrets.token_hex(32))"
    echo.
    echo FERNET_KEY:
    python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
    echo.
    echo Opening .env in Notepad. Fill in the keys above,
    echo set ADMIN_EMAIL and ADMIN_PASSWORD, then save.
    pause
    notepad .env
)
echo.

REM ── Database ──────────────────────────────────
echo [5/5] Setting up database...
echo (Requires PostgreSQL running and DATABASE_URL set in .env —
echo  see SETUP_GUIDE.txt step 8b if you have not created the DB yet.)
echo (Also requires Redis running and REDIS_URL set in .env —
echo  see SETUP_GUIDE.txt "PRODUCTION / MULTI-USER ARCHITECTURE".
echo  The app now runs as TWO processes: web (this) + worker.py —
echo  run both with: scripts\run.sh, or start worker.py separately.)
python scripts\init_db.py
if errorlevel 1 (
    echo ERROR: Database setup failed.
    echo Make sure .env has valid FERNET_KEY.
    pause
    exit /b 1
)
echo.

echo ============================================
echo   Setup Complete!
echo ============================================
echo.
echo To start: double-click run.bat
echo Then open: http://localhost:8000
echo.
pause
