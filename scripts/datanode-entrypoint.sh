#!/bin/bash

# Namenode가 준비될 때까지 대기
HOST="namenode"
PORT="8020"
MAX_RETRIES=30
RETRY_DELAY=10

for i in $(seq 1 $MAX_RETRIES);
do
  nc -z $HOST $PORT > /dev/null 2>&1
  result=$?
  if [ $result -eq 0 ]; then
    echo "Namenode ($HOST:$PORT) is ready!"
    break
  else
    echo "Waiting for Namenode ($HOST:$PORT)... (Attempt $i/$MAX_RETRIES)"
    sleep $RETRY_DELAY
  fi
  if [ $i -eq $MAX_RETRIES ]; then
    echo "Error: Namenode did not become ready after $MAX_RETRIES attempts."
    exit 1
  fi
done

# Namenode 주소를 명시적으로 설정 (core-site.xml에 반영)
# 이 설정은 Datanode가 Namenode를 찾을 때 사용됩니다.
# /opt/hadoop-3.2.1/etc/hadoop/core-site.xml 파일에 직접 설정하는 효과
xml_config="<configuration><property><name>fs.defaultFS</name><value>hdfs://namenode:8020</value></property><property><name>dfs.namenode.rpc-address</name><value>namenode:8020</value></property><property><name>dfs.namenode.servicerpc-address</name><value>namenode:8020</value></property></configuration>"
echo "$xml_config" > /opt/hadoop-3.2.1/etc/hadoop/core-site.xml

# 원본 Datanode 엔트리포인트 실행
exec /entrypoint.sh /run.sh
