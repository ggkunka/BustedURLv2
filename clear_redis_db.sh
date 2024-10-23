#!/bin/bash

# Path to redis-cli
REDIS_CLI="/home/yzhang10/redis-stable/src/redis-cli"

# Host and port of Redis server
REDIS_HOST="localhost"
REDIS_PORT="6379"

# Clear the current Redis database
echo "Clearing Redis database on $REDIS_HOST:$REDIS_PORT..."
$REDIS_CLI -h $REDIS_HOST -p $REDIS_PORT FLUSHALL

# Check the status
if [ $? -eq 0 ]; then
    echo "Redis database cleared successfully."
else
    echo "Failed to clear Redis database."
    exit 1
fi
