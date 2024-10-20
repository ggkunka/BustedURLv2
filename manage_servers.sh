#!/bin/bash

# Define paths to configurations and executables
ZOOKEEPER_CONFIG="/home/yzhang10/kafka/config/zookeeper.properties"
KAFKA_CONFIG="/home/yzhang10/kafka/config/server.properties"
REDIS_CONFIG="/home/yzhang10/redis-stable/redis.conf"
REDIS_SERVER="/home/yzhang10/redis-stable/src/redis-server"
CELERY_WORKER_PATH="/home/yzhang10/BustedURLv2/celery_worker.py"

start_zookeeper() {
    echo "Starting Zookeeper..."
    /home/yzhang10/kafka/bin/zookeeper-server-start.sh $ZOOKEEPER_CONFIG &
    sleep 5
}

start_kafka() {
    echo "Starting Kafka..."
    /home/yzhang10/kafka/bin/kafka-server-start.sh $KAFKA_CONFIG &
    sleep 5
}

start_redis() {
    echo "Starting Redis..."
    $REDIS_SERVER $REDIS_CONFIG &
    sleep 5
}

start_celery() {
    echo "Starting Celery worker..."
    celery -A $CELERY_WORKER_PATH worker --loglevel=info &
    sleep 5
}

stop_zookeeper() {
    echo "Stopping Zookeeper..."
    /home/yzhang10/kafka/bin/zookeeper-server-stop.sh
}

stop_kafka() {
    echo "Stopping Kafka..."
    /home/yzhang10/kafka/bin/kafka-server-stop.sh
}

stop_redis() {
    echo "Stopping Redis..."
    pkill -f $REDIS_SERVER
}

stop_celery() {
    echo "Stopping Celery worker..."
    pkill -f "celery"
}

start_all() {
    echo "Starting all servers..."
    start_zookeeper
    start_kafka
    start_redis
    start_celery
    echo "All servers started."
}

stop_all() {
    echo "Stopping all servers..."
    stop_celery
    stop_kafka
    stop_redis
    stop_zookeeper
    echo "All servers stopped."
}

# Display usage
usage() {
    echo "Usage: $0 {start|stop|restart}"
    exit 1
}

# Main script
case "$1" in
    start)
        start_all
        ;;
    stop)
        stop_all
        ;;
    restart)
        stop_all
        start_all
        ;;
    *)
        usage
        ;;
esac
