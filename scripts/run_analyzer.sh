#!/bin/bash

# docker-compose.yml의 depends_on과 healthcheck가 서비스 준비 상태를 보장하므로
# 스크립트 내부의 대기 로직은 제거합니다.

echo "Starting Spark Streaming Analyzer..."

# Spark Streaming Analyzer 실행
# 스크립트 경로는 docker-compose.yml에서 마운트한 /opt/bitnami/spark/scripts/ 입니다.
/opt/bitnami/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-client-runtime:3.3.4 \
  /opt/bitnami/spark/scripts/sentiment_analyzer.py
