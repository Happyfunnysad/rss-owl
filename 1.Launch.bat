@echo off
echo Starting installation and setup...

REM Check for Python
python --version >nul 2>&1
if errorlevel 1 (
    echo Python is not installed! Please install Python 3.8 or higher.
    echo Download from https://www.python.org/downloads/
    pause
    exit /b 1
)

REM Create and activate the virtual environment
echo Creating virtual environment...
python -m venv .venv
call venv\Scripts\activate.bat

REM Updating pip
echo Updating pip...
python -m pip install --upgrade pip

REM Install dependencies
echo Installing requirements...
pip install -r requirements.txt

REM Check if the installation was successful
if errorlevel 1 (
    echo Error installing dependencies!
    pause
    exit /b 1
)

echo Installation completed successfully!
echo Starting the application...

REM Starting the application
python Rsspars.py

REM If the application closes with an error, display the message
if errorlevel 1 (
    echo An error occurred while starting the application!
    pause
)


deactivate

Translated with DeepL.com (free version)