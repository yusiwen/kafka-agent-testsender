@echo off

set "CURRENT_DIR=%cd%"
set "JAVA_BIN=%JAVA_HOME%\bin\java.exe"
set "JAVA_OPTS=-Xmx256M -Xms256M"
set "JAR_FILE=%CURRENT_DIR%\lib\TestServer.jar"

%JAVA_BIN% %JAVA_OPTS% -jar %JAR_FILE% --port 3000