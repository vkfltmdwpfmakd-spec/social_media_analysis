import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, current_timestamp, date_sub, to_timestamp, explode, lower, regexp_replace, when, length, split
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import sys
import json

# 로깅 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# --- Spark 설정 ---
APP_NAME = "DailySentimentAggregator"
HDFS_INPUT_PATH = "hdfs://namenode:9000/user/spark/social_sentiment_data"
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/user/spark/daily_sentiment_reports"

def create_spark_session():
    """SparkSession을 생성하고 반환합니다."""
    try:
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
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
        # 지난 24시간 데이터 필터링
        # created_at은 UTC 기준이므로, 현재 UTC 시간에서 24시간 전을 기준으로 필터링
        # Spark의 current_timestamp()는 UTC 기준
        df = spark.read.parquet(HDFS_INPUT_PATH)
        
        # created_at 컬럼이 TimestampType이 아닐 경우 변환
        if not isinstance(df.schema["created_at"].dataType, TimestampType):
            df = df.withColumn("created_at", to_timestamp(col("created_at")))

        # 지난 24시간 데이터 필터링
        # Spark의 current_timestamp()는 UTC 기준
        # created_at은 producer에서 UTC로 저장했으므로, UTC 기준으로 비교
        df_filtered = df.filter(col("created_at") >= date_sub(current_timestamp(), 1))

        if df_filtered.isEmpty():
            logging.warning(f"지난 24시간 동안 수집된 데이터가 없습니다. 작업을 종료합니다.")
            return
        logging.info(f"지난 24시간 동안 {df_filtered.count()}개의 레코드를 읽었습니다.")

        # 1. 평균 감성 점수 계산
        # sentiment_label이 'positive', 'neutral', 'negative'라고 가정
        # 이를 수치화하여 평균 계산 (예: positive=1, neutral=0, negative=-1)
        sentiment_score_numeric_df = df_filtered.withColumn(
            "sentiment_numeric",
            when(col("sentiment_label") == "positive", 1.0)
            .when(col("sentiment_label") == "neutral", 0.0)
            .when(col("sentiment_label") == "negative", -1.0)
            .otherwise(0.0) # unknown 등은 0으로 처리
        )
        avg_sentiment_df = sentiment_score_numeric_df.agg(avg("sentiment_numeric").alias("average_sentiment_score"))
        avg_sentiment_score = avg_sentiment_df.collect()[0]["average_sentiment_score"]
        logging.info(f"지난 24시간 평균 감성 점수: {avg_sentiment_score:.4f}")

        # 2. 가장 많이 언급된 키워드 10개 추출
        # 텍스트 정제 및 토큰화 (간단한 예시)
        words_df = df_filtered.select(explode(split(lower(regexp_replace(col("text"), "[^a-z0-9가-힣 ]", "")), " ")).alias("word"))
        
        # 불용어 제거 (간단한 예시, 필요시 확장)
        stopwords = ["the", "a", "an", "is", "it", "for", "and", "in", "of", "to", "on", "with", "as", "by", "from", "at", "that", "this", "be", "have", "has", "had", "do", "does", "did", "not", "no", "yes", "i", "you", "he", "she", "it", "we", "they", "me", "him", "her", "us", "them", "my", "your", "his", "her", "its", "our", "their", "this", "that", "these", "those", "am", "are", "was", "were", "been", "being", "can", "could", "will", "would", "should", "may", "might", "must", "about", "above", "after", "again", "against", "all", "any", "because", "before", "being", "below", "between", "both", "but", "by", "down", "during", "each", "few", "for", "from", "further", "here", "how", "if", "into", "just", "more", "most", "now", "off", "once", "only", "or", "other", "our", "out", "over", "own", "same", "she", "so", "some", "such", "than", "that", "then", "there", "these", "they", "this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "we", "what", "when", "where", "which", "while", "who", "whom", "why", "with", "you", "your"]
        words_df = words_df.filter(~col("word").isin(stopwords))
        words_df = words_df.filter(length(col("word")) > 1) # 한 글자 단어 제거

        top_keywords_df = words_df.groupBy("word").agg(count("word").alias("count")).orderBy(col("count").desc()).limit(10)
        top_keywords = top_keywords_df.collect()
        logging.info("가장 많이 언급된 키워드 10개:")
        for row in top_keywords:
            logging.info(f"  - {row['word']}: {row['count']}회")

        # 결과 저장 (HDFS에 일별 리포트 저장)
        report_data = [
            {"report_date": current_timestamp(), "average_sentiment_score": avg_sentiment_score, "top_keywords": json.dumps([row.asDict() for row in top_keywords])}
        ]
        report_df = spark.createDataFrame(report_data)
        report_df.write \
            .mode("append") \
            .parquet(settings.DAILY_REPORT_PATH)
        logging.info(f"일별 감성 리포트가 HDFS 경로 '{settings.DAILY_REPORT_PATH}'에 성공적으로 저장되었습니다.")

    except Exception as e:
        logging.error("일별 감성 리포트 생성 중 오류 발생", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        aggregate_sentiment_data(spark)
        spark.stop()
