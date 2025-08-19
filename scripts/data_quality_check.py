import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, date_sub, to_timestamp, TimestampType
import os
import sys

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

APP_NAME = "DataQualityChecker"

def create_spark_session():
    """SparkSession을 생성하고 반환합니다."""
    try:
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.hadoop.fs.defaultFS", settings.HDFS_RPC_URL) \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        logging.info(f"[{APP_NAME}] SparkSession이 성공적으로 생성되었습니다.")
        return spark
    except Exception as e:
        logging.error(f"SparkSession 생성 실패: {e}")
        return None

def check_data_quality(spark):
    logging.info(f"HDFS 경로 '{settings.SENTIMENT_DATA_PATH}'의 데이터 품질을 확인합니다...")
    try:
        # 지난 1시간 동안 데이터가 수집되었는지 확인
        df = spark.read.parquet(settings.SENTIMENT_DATA_PATH)
        
        # created_at 컬럼이 TimestampType이 아닐 경우 변환
        if not isinstance(df.schema["created_at"].dataType, TimestampType):
            df = df.withColumn("created_at", to_timestamp(col("created_at")))

        df_recent = df.filter(col("created_at") >= date_sub(current_timestamp(), 1/24)) # 지난 1시간

        record_count = df_recent.count()
        if record_count == 0:
            logging.error("오류: 지난 1시간 동안 수집된 데이터가 없습니다. 데이터 파이프라인에 문제가 있을 수 있습니다.")
            raise Exception("No recent data in HDFS")
        else:
            logging.info(f"성공: 지난 1시간 동안 {record_count}개의 데이터가 HDFS에 정상적으로 수집되었습니다.")

    except Exception as e:
        logging.error(f"데이터 품질 검사 중 오류 발생: {e}", exc_info=True)
        raise # Airflow 태스크 실패를 위해 예외 다시 발생

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        check_data_quality(spark)
        spark.stop()
