#!/usr/bin/env bash

JAVA_BIN=$(which java)
JAVA_OPTS="-Xmx256M -Xms256M"
JAR_FILE="./lib/TestSender.jar"

eval $JAVA_BIN $JAVA_OPTS -jar $JAR_FILE --brokers 172.16.62.219:9092 --topic topic_yusiwen_tofsu --message-file ./message/sample.json "$@"
