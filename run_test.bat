@echo off
echo Starting HBase IoT Test Program...

REM 使用Maven运行测试程序
mvn clean compile exec:java -Dexec.mainClass="com.nexuscale.test.HBaseTestProgram"

pause 