import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, expr, to_timestamp
from pyspark.sql.types import TimestampType
import os
import sys

# 프로젝트 모듈 import
try:
    from config import settings
    from config.logging_config import get_logger
except ImportError:
    # Spark 환경에서의 폴백 설정
    class DefaultSettings:
        ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
        LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    
    settings = DefaultSettings()
    
    def get_logger(name):
        logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))
        return logging.getLogger(name)

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

APP_NAME = "DataQualityChecker"

def create_spark_session():
    """SparkSession을 생성하고 반환합니다."""
    try:
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logging.info(f"[{APP_NAME}] SparkSession이 성공적으로 생성되었습니다.")
        return spark
    except Exception as e:
        logging.error(f"SparkSession 생성 실패: {e}")
        return None

def check_data_quality(spark):
    logging.info(f"HDFS 경로 'hdfs://namenode:8020/user/spark/social_sentiment_data'의 데이터 품질을 확인합니다...")
    try:
        # 지난 1시간 동안 데이터가 수집되었는지 확인
        df = spark.read.parquet("hdfs://namenode:8020/user/spark/social_sentiment_data")
        
        # created_at 컬럼이 TimestampType이 아닐 경우 변환
        if not isinstance(df.schema["created_at"].dataType, TimestampType):
            df = df.withColumn("created_at", to_timestamp(col("created_at")))

        df_recent = df.filter(col("created_at") >= expr("current_timestamp() - interval 1 hour")) # 지난 1시간

        record_count = df_recent.count()
        if record_count == 0:
            logging.error("오류: 지난 1시간 동안 수집된 데이터가 없습니다. 데이터 파이프라인에 문제가 있을 수 있습니다.")
            # This script is for checking, so we don't raise an exception to halt the main pipeline
            # But for a DAG task, you might want to raise an exception.
            # For this implementation, we just log an error.
        else:
            logging.info(f"성공: 지난 1시간 동안 {record_count}개의 데이터가 HDFS에 정상적으로 수집되었습니다.")

    except Exception as e:
        logging.error(f"데이터 품질 검사 중 오류 발생: {e}", exc_info=True)
        # In a real-world scenario, you might want to raise an exception to fail the Airflow task
        # raise e 

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        try:
            check_data_quality(spark)
        finally:
            spark.stop()