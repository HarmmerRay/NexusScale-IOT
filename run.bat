@echo off
echo Starting NexusScale IoT Device Data Consumer...

REM Set classpath
set CLASSPATH=target\lib\*;target\classes

REM Check if classes exist
if not exist "target\classes\com\nexuscale\Main.class" (
    echo Error: Project not compiled. Please run compile.bat first.
    pause
    exit /b 1
)

REM Run the application
java -cp "%CLASSPATH%" com.nexuscale.Main

pause 