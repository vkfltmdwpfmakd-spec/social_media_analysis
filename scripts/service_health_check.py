import logging
import socket
import sys
from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable
from hdfs import InsecureClient
import os

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def check_kafka_health():
    logging.info("Kafka 브로커 상태 확인 중...")
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=settings.KAFKA_BROKER, request_timeout_ms=5000)
        brokers = admin_client.bootstrap_connected()
        if brokers:
            logging.info("Kafka 브로커 연결 성공.")
            return True
        else:
            logging.error("Kafka 브로커에 연결할 수 없습니다.")
            return False
    except NoBrokersAvailable:
        logging.error(f"Kafka 브로커({settings.KAFKA_BROKER})를 찾을 수 없습니다.")
        return False
    except Exception as e:
        logging.error(f"Kafka 상태 확인 중 오류 발생: {e}", exc_info=True)
        return False

def check_spark_master_health():
    logging.info("Spark Master 상태 확인 중...")
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((settings.SPARK_MASTER_HOST, settings.SPARK_MASTER_PORT))
            logging.info("Spark Master 연결 성공.")
            return True
    except ConnectionRefusedError:
        logging.error(f"Spark Master({settings.SPARK_MASTER_HOST}:{settings.SPARK_MASTER_PORT}) 연결 거부됨.")
        return False
    except socket.timeout:
        logging.error(f"Spark Master({settings.SPARK_MASTER_HOST}:{settings.SPARK_MASTER_PORT}) 연결 시간 초과.")
        return False
    except Exception as e:
        logging.error(f"Spark Master 상태 확인 중 오류 발생: {e}", exc_info=True)
        return False

def check_hdfs_namenode_health():
    logging.info("HDFS Namenode 상태 확인 중...")
    try:
        client = InsecureClient(settings.HDFS_WEB_URL)
        # HDFS 루트 디렉토리 목록을 가져와 연결 확인
        client.list('/')
        logging.info("HDFS Namenode 연결 성공.")
        return True
    except Exception as e:
        logging.error(f"HDFS Namenode({settings.HDFS_WEB_URL}) 연결 실패: {e}", exc_info=True)
        return False

if __name__ == "__main__":
    all_healthy = True

    if not check_kafka_health():
        all_healthy = False
    
    if not check_spark_master_health():
        all_healthy = False

    if not check_hdfs_namenode_health():
        all_healthy = False

    if not all_healthy:
        logging.error("오류: 하나 이상의 핵심 서비스에 문제가 감지되었습니다. 파이프라인 상태를 확인하세요.")
        sys.exit(1) # Airflow 태스크 실패
    else:
        logging.info("모든 핵심 서비스가 정상적으로 작동 중입니다.")
