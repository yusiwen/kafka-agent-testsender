#!/usr/bin/env bash

JAVA_BIN=$(which java)
JAVA_OPTS="-Xmx256M -Xms256M"
JAR_FILE="./lib/TestServer.jar"

eval $JAVA_BIN $JAVA_OPTS -jar $JAR_FILE --port 3000
