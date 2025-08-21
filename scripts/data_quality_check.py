"""
데이터 품질 검증 모듈 - 실시간 데이터 파이프라인 모니터링

HDFS에 저장된 소셜 미디어 감성 분석 데이터의 품질과 수집 상태를 확인합니다.
지난 1시간 동안 데이터가 정상적으로 수집되고 있는지 모니터링하여
데이터 파이프라인의 건강성을 보장합니다.

주요 기능:
- HDFS 데이터 존재 여부 확인
- 최근 수집된 데이터 개수 확인
- 타임스탬프 데이터 유효성 검증
- 데이터 품질 문제 시 알림
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, to_timestamp
from pyspark.sql.types import TimestampType
import os
import sys

# 프로젝트 설정 모듈 import - 중앙 집중식 설정 관리
try:
    from config import settings
    from config.logging_config import get_logger
except ImportError:
    # Spark 워커 노드에서 config 모듈을 찾을 수 없을 때의 폴백 설정
    # Docker 환경에서는 때때로 Python 경로 문제가 발생할 수 있음
    class DefaultSettings:
        ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
        LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    settings = DefaultSettings()
    
    def get_logger(name):
        logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))
        return logging.getLogger(name)

# 로깅 설정 - 품질 검증 과정을 추적하기 위한 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

APP_NAME = "DataQualityChecker"  # Spark UI에서 표시될 애플리케이션 이름

def create_spark_session():
    """
    데이터 품질 검증을 위한 Spark 세션을 생성합니다.
    
    HDFS 연결 설정과 함께 Spark 세션을 초기화하여
    분산 데이터 처리가 가능하도록 구성합니다.
    """
    try:
        # Spark 세션 생성 - 분산 데이터 처리를 위한 진입점
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
            .getOrCreate()
            
        # 불필요한 로그 출력 최소화 - 품질 검증 로그에 집중
        spark.sparkContext.setLogLevel("WARN")
        logging.info(f"[{APP_NAME}] SparkSession이 성공적으로 생성되었습니다.")
        return spark
    except Exception as e:
        logging.error(f"SparkSession 생성 실패: {e}")
        return None

def check_data_quality(spark):
    """
    실시간 데이터 파이프라인의 품질을 검증합니다.
    
    지난 1시간 동안 수집된 데이터의 양과 품질을 확인하여
    데이터 파이프라인이 정상적으로 동작하고 있는지 모니터링합니다.
    """
    logging.info(f"HDFS 경로 'hdfs://namenode:8020/user/spark/social_sentiment_data'의 데이터 품질을 확인합니다...")
    try:
        # HDFS에서 Spark Streaming이 저장한 모든 Parquet 파일 읽기
        # 실시간으로 계속 업데이트되는 데이터를 확인하기 위함
        df = spark.read.parquet("hdfs://namenode:8020/user/spark/social_sentiment_data")
        
        # 타임스탬프 컬럼 타입 확인 및 변환
        # 때로는 문자열로 저장된 시간 데이터를 TimestampType으로 변환해야 함
        if not isinstance(df.schema["created_at"].dataType, TimestampType):
            df = df.withColumn("created_at", to_timestamp(col("created_at")))

        # 지난 1시간 동안 수집된 데이터만 필터링
        # 실시간 파이프라인이 정상적으로 동작하는지 확인하는 기준
        df_recent = df.filter(col("created_at") >= expr("current_timestamp() - interval 1 hour"))

        record_count = df_recent.count()
        if record_count == 0:
            # 데이터가 없으면 심각한 파이프라인 문제 가능성
            logging.error("⚠️ 오류: 지난 1시간 동안 수집된 데이터가 없습니다. 데이터 파이프라인에 문제가 있을 수 있습니다.")
            # 모니터링 목적이므로 예외를 발생시키지 않고 로그만 남김
            # Airflow DAG에서는 이 상황을 감지하여 알림을 보낼 수 있음
        else:
            # 정상적인 데이터 수집 확인
            logging.info(f"✅ 성공: 지난 1시간 동안 {record_count}개의 데이터가 HDFS에 정상적으로 수집되었습니다.")

    except Exception as e:
        logging.error(f"데이터 품질 검사 중 오류 발생: {e}", exc_info=True)
        # 실제 운영 환경에서는 이 예외를 다시 발생시켜 Airflow 작업을 실패시킬 수 있음
        # 현재는 모니터링 목적이므로 로그만 남기고 계속 진행
        # raise e  # 필요시 주석 해제하여 작업 실패 처리 가능 

if __name__ == "__main__":
    # 메인 실행 부분 - Airflow DAG에서 호출되는 진입점
    spark = create_spark_session()
    if spark:
        try:
            # 데이터 품질 검증 실행
            check_data_quality(spark)
        finally:
            # 작업 완료 후 Spark 세션 정리 (리소스 해제)
            spark.stop()
    else:
        logging.error("SparkSession 생성에 실패하여 데이터 품질 검증을 수행할 수 없습니다.")