#!/bin/bash

# Define function to start servers
start_zookeeper() {
    echo "Starting Zookeeper..."
    /home/yzhang10/kafka/bin/zookeeper-server-start.sh /home/yzhang10/kafka/config/zookeeper.properties > /dev/null 2>&1 &
    ZOOKEEPER_PID=$!
    echo "Zookeeper started with PID $ZOOKEEPER_PID."
    sleep 10
}

start_kafka() {
    echo "Starting Kafka..."
    /home/yzhang10/kafka/bin/kafka-server-start.sh /home/yzhang10/kafka/config/server.properties > /dev/null 2>&1 &
    KAFKA_PID=$!
    echo "Kafka started with PID $KAFKA_PID."
    sleep 10
}

start_celery() {
    echo "Starting Celery worker..."
    celery -A celery_worker worker --loglevel=info > /dev/null 2>&1 &
    CELERY_PID=$!
    echo "Celery started with PID $CELERY_PID."
    sleep 5
}

create_kafka_topic() {
    echo "Creating Kafka topic 'predictions'..."
    /home/yzhang10/kafka/bin/kafka-topics.sh --create --topic predictions --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1 > /dev/null 2>&1
    echo "Kafka topic 'predictions' created."
}

# Define function to stop servers
stop_process() {
    local PID=$1
    local NAME=$2
    if ps -p $PID > /dev/null; then
        echo "Stopping $NAME with PID $PID..."
        kill -SIGTERM $PID
        wait $PID
        echo "$NAME stopped."
    else
        echo "$NAME is not running."
    fi
}

# Parse input arguments
if [ "$1" == "start" ]; then
    echo "Starting all servers..."
    start_zookeeper
    start_kafka
    create_kafka_topic
    start_celery
    echo "All servers started."

elif [ "$1" == "stop" ]; then
    echo "Stopping all servers..."
    stop_process $ZOOKEEPER_PID "Zookeeper"
    stop_process $KAFKA_PID "Kafka"
    stop_process $CELERY_PID "Celery"
    echo "All servers stopped."

else
    echo "Usage: $0 {start|stop}"
    exit 1
fi
