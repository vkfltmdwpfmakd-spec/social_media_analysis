"""
일일 감성 분석 집계기 - 배치 처리 모듈

지난 24시간 동안 수집된 소셜 미디어 감성 분석 데이터를 집계하여
일별 리포트를 생성하는 Spark 배치 작업입니다.

주요 기능:
- HDFS에 저장된 실시간 감성 분석 결과를 읽어옴
- 지난 24시간 데이터만 필터링하여 분석
- 평균 감성 점수 계산 (+1: 긍정, 0: 중성, -1: 부정)
- 가장 많이 언급된 키워드 Top 10 추출
- 일별 리포트로 저장하여 대시보드에서 활용
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp, date_sub, to_timestamp, explode, lower, regexp_replace, when, length, split
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import sys
import json
import os
from datetime import datetime

# 프로젝트 모듈 import - 설정 파일에서 HDFS 경로 정보 가져오기
try:
    from config import settings
except ImportError:
    # Spark 컨테이너 환경에서 config 모듈을 찾을 수 없을 때의 폴백 설정
    # Docker 환경에서는 Python 경로 문제로 때로는 직접 설정이 필요함
    class DefaultSettings:
        HDFS_RPC_URL = "hdfs://namenode:8020"
        SENTIMENT_DATA_PATH = "hdfs://namenode:8020/user/spark/social_sentiment_data"
        DAILY_REPORT_PATH = "hdfs://namenode:8020/user/spark/daily_sentiment_reports"
    
    settings = DefaultSettings()

# 로깅 설정 - 배치 작업 진행 상황을 추적하기 위한 로그 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- Spark 애플리케이션 설정 ---
APP_NAME = "DailySentimentAggregator"  # Spark UI에서 표시될 애플리케이션 이름
HDFS_INPUT_PATH = settings.SENTIMENT_DATA_PATH   # 실시간 감성 분석 결과가 저장된 경로
HDFS_OUTPUT_PATH = settings.DAILY_REPORT_PATH    # 일별 집계 리포트를 저장할 경로

def create_spark_session():
    """
    Spark 세션을 생성하고 HDFS 연결을 설정합니다.
    
    배치 처리를 위한 Spark 설정:
    - HDFS를 기본 파일 시스템으로 설정
    - 로그 레벨을 WARN으로 설정하여 불필요한 로그 출력 최소화
    """
    try:
        # SparkSession 생성 - 분산 처리를 위한 Spark 클러스터 연결
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.hadoop.fs.defaultFS", settings.HDFS_RPC_URL) \
            .getOrCreate()
            
        # Spark 로그 레벨 조정 - 너무 많은 DEBUG 로그를 방지
        spark.sparkContext.setLogLevel("WARN")
        logging.info(f"[{APP_NAME}] SparkSession이 성공적으로 생성되었습니다.")
        return spark
    except Exception as e:
        logging.error(f"SparkSession 생성 실패: {e}")
        return None

def aggregate_sentiment_data(spark):
    """
    지난 24시간 동안의 감성 분석 데이터를 집계하여 일별 리포트를 생성합니다.
    
    처리 과정:
    1. HDFS에서 Parquet 파일들을 읽어옴
    2. 지난 24시간 데이터만 필터링
    3. 평균 감성 점수 계산
    4. 키워드 빈도 분석
    5. 결과를 일별 리포트로 저장
    """
    logging.info(f"HDFS 경로 '{HDFS_INPUT_PATH}'에서 데이터를 읽어옵니다...")
    try:
        # Spark Streaming이 저장한 모든 Parquet 파일들을 읽어옴
        # Parquet은 분산 환경에서 효율적인 컬럼형 저장 포맷
        df = spark.read.parquet(HDFS_INPUT_PATH)
        
        # 타임스탬프 컬럼 타입 확인 및 변환
        # 때로는 문자열로 저장된 시간 데이터를 TimestampType으로 변환해야 함
        if not isinstance(df.schema["created_at"].dataType, TimestampType):
            df = df.withColumn("created_at", to_timestamp(col("created_at")))

        # 지난 24시간 데이터만 필터링 - 일별 리포트이므로 최근 하루치만 분석
        df_filtered = df.filter(col("created_at") >= date_sub(current_timestamp(), 1))

        # 데이터 존재 여부 확인 - 빈 데이터셋이면 작업 중단
        if df_filtered.isEmpty():
            logging.warning(f"지난 24시간 동안 수집된 데이터가 없습니다. 작업을 종료합니다.")
            return
        record_count = df_filtered.count()
        logging.info(f"지난 24시간 동안 {record_count}개의 레코드를 읽었습니다.")

        # 감성 라벨을 수치로 변환하여 평균 계산 가능하게 만듦
        # Positive: +1, Neutral: 0, Negative: -1 스케일 사용
        # 이렇게 하면 전체적인 감성 경향을 하나의 점수로 표현 가능
        sentiment_score_numeric_df = df_filtered.withColumn(
            "sentiment_numeric",
            when(lower(col("sentiment_label")) == "positive", 1.0)
            .when(lower(col("sentiment_label")) == "neutral", 0.0)
            .when(lower(col("sentiment_label")) == "negative", -1.0)
            .otherwise(0.0)  # 기타 값들은 중성으로 처리
        )
        
        # 전체 데이터의 평균 감성 점수 계산
        # 결과가 양수면 전반적으로 긍정적, 음수면 부정적 경향
        avg_sentiment_df = sentiment_score_numeric_df.agg(avg("sentiment_numeric").alias("average_sentiment_score"))
        avg_sentiment_score = avg_sentiment_df.collect()[0]["average_sentiment_score"]
        logging.info(f"지난 24시간 평균 감성 점수: {avg_sentiment_score:.4f}")

        # 키워드 추출을 위한 텍스트 전처리 과정
        # 1. 소문자 변환, 특수문자 제거 (영어, 숫자, 한글만 남김)
        # 2. 공백으로 분할하여 개별 단어로 분리
        # 3. 각 단어를 별도의 행으로 확장 (explode)
        words_df = df_filtered.select(explode(split(lower(regexp_replace(col("text"), "[^a-z0-9가-힣 ]", " ")), " ")).alias("word"))
        
        # 불용어(stopwords) 목록 - 분석에 의미가 없는 일반적인 단어들
        # 관사, 전치사, 대명사 등 빈도는 높지만 내용상 의미가 적은 단어들을 제외
        stopwords = ["the", "a", "an", "is", "it", "for", "and", "in", "of", "to", "on", "with", "as", "by", "from", "at", "that", "this", "be", "have", "has", "had", "do", "does", "did", "not", "no", "yes", "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them", "my", "your", "his", "her", "its", "our", "their", "this", "that", "these", "those", "am", "are", "was", "were", "been", "being", "can", "could", "will", "would", "should", "may", "might", "must", "about", "above", "after", "again", "against", "all", "any", "because", "before", "being", "below", "between", "both", "but", "by", "down", "during", "each", "few", "for", "from", "further", "here", "how", "if", "into", "just", "more", "most", "now", "off", "once", "only", "or", "other", "our", "out", "over", "own", "same", "she", "so", "some", "such", "than", "that", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "what", "when", "where", "which", "while", "who", "whom", "why", "with", "you", "your"]
        
        # 불용어 및 한글자 단어 제거
        words_df = words_df.filter(~col("word").isin(stopwords))  # 불용어 제거
        words_df = words_df.filter(length(col("word")) > 1)        # 한글자 단어 제거

        # 단어별 빈도 계산 및 상위 10개 키워드 추출
        # 가장 많이 언급된 키워드를 통해 주요 화제나 이슈를 파악할 수 있음
        top_keywords_df = words_df.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc()).limit(10)
        top_keywords = top_keywords_df.collect()
        
        logging.info("가장 많이 언급된 키워드 10개:")
        for row in top_keywords:
            logging.info(f"  - {row['word']}: {row['count']}회")

        # 일별 리포트 데이터 구성
        # 리포트 생성 시간, 평균 감성 점수, 상위 키워드를 하나의 레코드로 저장
        report_generation_time = datetime.now()
        report_data = [
            (report_generation_time, avg_sentiment_score, json.dumps([row.asDict() for row in top_keywords]))
        ]
        
        # 리포트 데이터 스키마 정의
        # - report_date: 리포트 생성 시간
        # - average_sentiment_score: 평균 감성 점수 (-1.0 ~ +1.0)
        # - top_keywords: JSON 형태로 저장된 상위 키워드 목록
        schema = StructType([
            StructField("report_date", TimestampType(), True),
            StructField("average_sentiment_score", FloatType(), True),
            StructField("top_keywords", StringType(), True)
        ])

        # DataFrame 생성 후 HDFS에 Parquet 형태로 저장
        # append 모드로 저장하여 기존 리포트들과 함께 누적 보관
        report_df = spark.createDataFrame(report_data, schema=schema)
        report_df.write \
            .mode("append") \
            .parquet(HDFS_OUTPUT_PATH)
        logging.info(f"일별 감성 리포트가 HDFS 경로 '{HDFS_OUTPUT_PATH}'에 성공적으로 저장되었습니다.")

    except Exception as e:
        logging.error("일별 감성 리포트 생성 중 오류 발생", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    # 메인 실행 부분 - Airflow DAG에서 호출되는 진입점
    spark = create_spark_session()
    if spark:
        try:
            # 일별 감성 데이터 집계 실행
            aggregate_sentiment_data(spark)
        finally:
            # 작업 완료 후 Spark 세션 정리
            # 리소스 절약을 위해 반드시 stop() 호출 필요
            spark.stop()
    else:
        logging.error("SparkSession 생성에 실패하여 작업을 중단합니다.")