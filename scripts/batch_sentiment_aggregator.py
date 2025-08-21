import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp, date_sub, to_timestamp, explode, lower, regexp_replace, when, length, split
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import sys
import json
import os
from datetime import datetime

# 프로젝트 모듈 import
try:
    from config import settings
except ImportError:
    # Spark 환경에서의 폴백 설정
    class DefaultSettings:
        HDFS_RPC_URL = "hdfs://namenode:8020"
        SENTIMENT_DATA_PATH = "hdfs://namenode:8020/user/spark/social_sentiment_data"
        DAILY_REPORT_PATH = "hdfs://namenode:8020/user/spark/daily_sentiment_reports"
    
    settings = DefaultSettings()

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- Spark 설정 ---
APP_NAME = "DailySentimentAggregator"
HDFS_INPUT_PATH = settings.SENTIMENT_DATA_PATH
HDFS_OUTPUT_PATH = settings.DAILY_REPORT_PATH

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

def aggregate_sentiment_data(spark):
    logging.info(f"HDFS 경로 '{HDFS_INPUT_PATH}'에서 데이터를 읽어옵니다...")
    try:
        df = spark.read.parquet(HDFS_INPUT_PATH)
        
        if not isinstance(df.schema["created_at"].dataType, TimestampType):
            df = df.withColumn("created_at", to_timestamp(col("created_at")))

        df_filtered = df.filter(col("created_at") >= date_sub(current_timestamp(), 1))

        if df_filtered.isEmpty():
            logging.warning(f"지난 24시간 동안 수집된 데이터가 없습니다. 작업을 종료합니다.")
            return
        record_count = df_filtered.count()
        logging.info(f"지난 24시간 동안 {record_count}개의 레코드를 읽었습니다.")

        sentiment_score_numeric_df = df_filtered.withColumn(
            "sentiment_numeric",
            when(col("sentiment_label") == "positive", 1.0)
            .when(col("sentiment_label") == "neutral", 0.0)
            .when(col("sentiment_label") == "negative", -1.0)
            .otherwise(0.0)
        )
        avg_sentiment_df = sentiment_score_numeric_df.agg(avg("sentiment_numeric").alias("average_sentiment_score"))
        avg_sentiment_score = avg_sentiment_df.collect()[0]["average_sentiment_score"]
        logging.info(f"지난 24시간 평균 감성 점수: {avg_sentiment_score:.4f}")

        words_df = df_filtered.select(explode(split(lower(regexp_replace(col("text"), "[^a-z0-9가-힣 ]", " ")), " ")).alias("word"))
        
        stopwords = ["the", "a", "an", "is", "it", "for", "and", "in", "of", "to", "on", "with", "as", "by", "from", "at", "that", "this", "be", "have", "has", "had", "do", "does", "did", "not", "no", "yes", "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them", "my", "your", "his", "her", "its", "our", "their", "this", "that", "these", "those", "am", "are", "was", "were", "been", "being", "can", "could", "will", "would", "should", "may", "might", "must", "about", "above", "after", "again", "against", "all", "any", "because", "before", "being", "below", "between", "both", "but", "by", "down", "during", "each", "few", "for", "from", "further", "here", "how", "if", "into", "just", "more", "most", "now", "off", "once", "only", "or", "other", "our", "out", "over", "own", "same", "she", "so", "some", "such", "than", "that", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "what", "when", "where", "which", "while", "who", "whom", "why", "with", "you", "your"]
        words_df = words_df.filter(~col("word").isin(stopwords))
        words_df = words_df.filter(length(col("word")) > 1)

        top_keywords_df = words_df.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc()).limit(10)
        top_keywords = top_keywords_df.collect()
        logging.info("가장 많이 언급된 키워드 10개:")
        for row in top_keywords:
            logging.info(f"  - {row['word']}: {row['count']}회")

        report_generation_time = datetime.now()
        report_data = [
            (report_generation_time, avg_sentiment_score, json.dumps([row.asDict() for row in top_keywords]))
        ]
        
        schema = StructType([
            StructField("report_date", TimestampType(), True),
            StructField("average_sentiment_score", FloatType(), True),
            StructField("top_keywords", StringType(), True)
        ])

        report_df = spark.createDataFrame(report_data, schema=schema)
        report_df.write \
            .mode("append") \
            .parquet(HDFS_OUTPUT_PATH)
        logging.info(f"일별 감성 리포트가 HDFS 경로 '{HDFS_OUTPUT_PATH}'에 성공적으로 저장되었습니다.")

    except Exception as e:
        logging.error("일별 감성 리포트 생성 중 오류 발생", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        aggregate_sentiment_data(spark)
        spark.stop()