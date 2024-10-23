#!/bin/bash

# HDFS path to clear (you can specify the directory or file to delete)
HDFS_PATH="/phishing_urls/collected_urls.txt"

# HDFS bin location (if HDFS command is not in PATH, you can specify the full path)
HDFS_BIN="/home/yzhang10/hadoop/bin/hdfs"

# Remove the specified HDFS file or directory
echo "Removing files in HDFS path: $HDFS_PATH..."
$HDFS_BIN dfs -rm -r $HDFS_PATH

# Check the status
if [ $? -eq 0 ]; then
    echo "HDFS path cleared successfully."
else
    echo "Failed to clear HDFS path."
    exit 1
fi
