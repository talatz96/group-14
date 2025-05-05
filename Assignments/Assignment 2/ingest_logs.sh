#!/bin/bash


if [ -z "$1" ]; then
    echo "Usage: ./ingest_logs.sh YYYY-MM-DD"
    exit 1
fi


DATE=$1
YEAR=$(date -d "$DATE" +%Y)
MONTH=$(date -d "$DATE" +%m)
DAY=$(date -d "$DATE" +%d)

echo "Ingesting logs for $DATE into HDFS..."


hdfs dfs -mkdir -p /raw/logs/$YEAR/$MONTH/$DAY
hdfs dfs -mkdir -p /raw/metadata


hdfs dfs -put /data/user_activity_logs.csv /raw/logs/$YEAR/$MONTH/$DAY/
hdfs dfs -put /data/content_metadata.csv /raw/metadata/

echo "Data ingestion completed for $DATE."
