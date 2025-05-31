@echo off
echo Compiling NexusScale IoT Project...

REM Create directories
if not exist "target\classes" mkdir target\classes
if not exist "target\lib" mkdir target\lib
if not exist "logs" mkdir logs

REM Download dependencies (you need to download these JAR files manually)
echo Note: Please download the following JAR files and place them in target\lib\ directory:
echo - mysql-connector-java-8.0.33.jar
echo - jedis-4.4.3.jar
echo - jackson-databind-2.15.2.jar
echo - jackson-core-2.15.2.jar
echo - jackson-annotations-2.15.2.jar
echo - slf4j-api-1.7.36.jar
echo - logback-classic-1.2.12.jar
echo - logback-core-1.2.12.jar

REM Set classpath
set CLASSPATH=target\lib\*;target\classes

REM Compile Java files
echo Compiling Java source files...
javac -d target\classes -cp "%CLASSPATH%" src\main\java\com\nexuscale\*.java src\main\java\com\nexuscale\config\*.java src\main\java\com\nexuscale\database\*.java src\main\java\com\nexuscale\redis\*.java src\main\java\com\nexuscale\consumer\*.java src\main\java\com\nexuscale\service\*.java

if %ERRORLEVEL% EQU 0 (
    echo Compilation successful!
    echo Copy resources...
    xcopy /Y src\main\resources\* target\classes\
    echo.
    echo To run the application:
    echo java -cp "%CLASSPATH%" com.nexuscale.Main
) else (
    echo Compilation failed!
)

pause 