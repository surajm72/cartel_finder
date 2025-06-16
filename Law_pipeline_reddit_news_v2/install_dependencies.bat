@echo off
echo Legal Data Pipeline - Dependency Installer
echo -----------------------------------------
echo.

echo This script will install all required Python libraries.
echo Please make sure Python is installed and added to PATH.
echo.

echo Upgrading pip...
python -m pip install --upgrade pip

echo.
echo Installing required libraries...
pip install -r requirements.txt

echo.
echo Installation completed.
echo.

echo You may now run the pipeline using one of the run_*.bat files.
echo.

pause 