@echo off
echo ======================================
echo        HBase连接测试脚本
echo ======================================

REM 设置编码
chcp 65001 > nul

REM 检查Maven
echo 检查Maven环境...
mvn -version
if %errorlevel% neq 0 (
    echo Error: Maven未安装或未配置到PATH
    pause
    exit /b 1
)

REM 编译项目
echo.
echo 编译项目...
mvn clean compile -q
if %errorlevel% neq 0 (
    echo Error: 项目编译失败
    pause
    exit /b 1
)

echo.
echo 编译成功，正在测试HBase连接...
echo.

REM 运行HBase测试程序
mvn exec:java -Dexec.mainClass="com.nexuscale.test.HBaseTestProgram" -Dexec.args="" -q

echo.
echo 测试完成！
pause 