@echo off

set "CURRENT_DIR=%cd%"
set "JAVA_BIN=%JAVA_HOME%\bin\java.exe"
set "JAVA_OPTS=-Xmx256M -Xms256M"
set "JAR_FILE=%CURRENT_DIR%\lib\TestSender.jar"

%JAVA_BIN% %JAVA_OPTS% -jar %JAR_FILE% --brokers 172.16.62.219:9092 --topic topic_yusiwen_tofsu