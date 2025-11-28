@echo off
REM =====================================================
REM CSV Data Downloader - Streamlit App Launcher
REM =====================================================

title CSV Data Downloader

echo.
echo ========================================
echo  CSV Data Downloader
echo ========================================
echo.

REM Change to the script's directory
cd /d "%~dp0"

REM Check if Python is installed
echo [1/4] Checking Python installation...
python --version >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Python is not installed!
    echo.
    echo Please install Python from: https://www.python.org/downloads/
    echo Make sure to check "Add Python to PATH" during installation
    echo.
    pause
    exit /b 1
)
python --version
echo.

REM Check if main.py exists
echo [2/4] Checking application files...
if not exist "main.py" (
    echo [ERROR] main.py not found!
    echo Make sure main.py is in the same folder as this batch file.
    echo.
    pause
    exit /b 1
)
echo [OK] Application files found
echo.

REM Create/activate virtual environment
echo [3/4] Setting up virtual environment...
if not exist "vmac\Scripts\python.exe" (
    echo.
    echo First time setup - Creating virtual environment...
    echo This will take 2-3 minutes, please wait...
    echo.
    
    python -m venv vmac
    
    if errorlevel 1 (
        echo [ERROR] Failed to create virtual environment
        echo Try running as Administrator
        echo.
        pause
        exit /b 1
    )
    
    echo [OK] Virtual environment created
    echo.
    echo Installing packages...
    echo.
    
    REM Use full path to venv python
    vmac\Scripts\python.exe -m pip install --upgrade pip --quiet
    vmac\Scripts\python.exe -m pip install streamlit psycopg2-binary polars
    
    if errorlevel 1 (
        echo [ERROR] Package installation failed
        echo Check your internet connection
        echo.
        pause
        exit /b 1
    )
    
    echo.
    echo [SUCCESS] Installation complete!
    echo.
) else (
    echo [OK] Virtual environment exists
    echo.
)

REM Activate virtual environment
echo Activating environment...
call vmac\Scripts\activate.bat
if errorlevel 1 (
    echo [ERROR] Failed to activate virtual environment
    echo Try deleting the 'venv' folder and run again
    echo.
    pause
    exit /b 1
)
echo [OK] Environment activated
echo.

REM Verify streamlit is available
where streamlit >nul 2>&1
if errorlevel 1 (
    echo [ERROR] Streamlit not found in virtual environment
    echo Reinstalling packages...
    echo.
    vmac\Scripts\python.exe -m pip install streamlit psycopg2-binary polars --force-reinstall
    echo.
)

echo [4/4] Starting application...
echo.
echo ========================================
echo  RUNNING APPLICATION
echo ========================================
echo.
echo Browser will open at: http://localhost:8501
echo.
echo IMPORTANT: Keep this window open!
echo To stop: Close this window or press Ctrl+C
echo.
echo ========================================
echo.

REM Run streamlit using the virtual environment's streamlit
vmac\Scripts\streamlit.exe run main.py --server.port=8501 --server.address=localhost --browser.gatherUsageStats=false

REM If streamlit exits
echo.
echo ========================================
echo  Application stopped
echo ========================================
echo.
pause