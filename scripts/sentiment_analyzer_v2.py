"""
개선된 실시간 감성 분석 시스템 v2.0
- 고도화된 API 기반 감성 분석 통합
- 성능 최적화 및 메모리 관리 개선
- 강화된 에러 처리 및 복구 메커니즘
- 중앙 로깅 시스템 적용
"""

import sys
import os
import time
import uuid
import asyncio
from concurrent.futures import ThreadPoolExecutor
from typing import List, Dict, Any

# 프로젝트 모듈 import
try:
    from config import settings
    from config.logging_config import get_logger
except ImportError:
    # Spark 환경에서의 폴백 설정
    import os
    
    # 기본 설정 클래스 정의
    class DefaultSettings:
        ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
        LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
        HUGGINGFACE_API_KEY = os.getenv('HUGGINGFACE_API_KEY', '')
        MAX_API_REQUESTS_PER_MINUTE = int(os.getenv('MAX_API_REQUESTS_PER_MINUTE', '50'))
        API_TIMEOUT_SECONDS = int(os.getenv('API_TIMEOUT_SECONDS', '10'))
        SPARK_BATCH_INTERVAL = int(os.getenv('SPARK_BATCH_INTERVAL', '30'))
        MAX_FILES_PER_LOAD = int(os.getenv('MAX_FILES_PER_LOAD', '15'))
        PARQUET_COMPRESSION = os.getenv('PARQUET_COMPRESSION', 'snappy')
        DASHBOARD_REFRESH_SECONDS = int(os.getenv('DASHBOARD_REFRESH_SECONDS', '30'))
    
    settings = DefaultSettings()
    
    def get_logger(name):
        import logging
        logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))
        return logging.getLogger(name)

# PySpark 관련 import
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, pandas_udf, when, from_utc_timestamp,
    current_timestamp, length, trim, regexp_replace, from_unixtime
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import pandas as pd

# 고도화된 감성 분석 모듈 import (폴백 시스템 활성화)
from advanced_sentiment_analyzer import get_sentiment_analyzer, analyze_sentiment_batch_sync

# 애플리케이션 설정
APP_NAME = "AdvancedSentimentAnalyzer_v2"
OUTPUT_PATH = settings.SENTIMENT_DATA_PATH

# PySpark UDF 함수들 (클래스 밖에서 정의)
@pandas_udf(StringType())
def detect_language_optimized(texts: pd.Series) -> pd.Series:
    """다국어 지원 최적화된 언어 감지 UDF"""
    import re
    
    # 다양한 언어 패턴 정의
    korean_pattern = re.compile(r'[가-힣]')
    chinese_pattern = re.compile(r'[\u4e00-\u9fff]')  # 중국어 한자
    japanese_pattern = re.compile(r'[\u3040-\u309f\u30a0-\u30ff]')  # 히라가나, 가타카나
    arabic_pattern = re.compile(r'[\u0600-\u06ff\u0750-\u077f\u08a0-\u08ff\ufb50-\ufdff\ufe70-\ufeff]')  # 아랍어
    cyrillic_pattern = re.compile(r'[\u0400-\u04ff]')  # 키릴문자 (러시아어 등)
    thai_pattern = re.compile(r'[\u0e00-\u0e7f]')  # 태국어
    results = []
    
    for text in texts:
        if pd.isna(text) or not isinstance(text, str) or len(text.strip()) == 0:
            results.append("en")
            continue
        
        text_clean = text.replace(' ', '')
        total_chars = len(text_clean)
        
        if total_chars == 0:
            results.append("en")
            continue
        
        # 각 언어별 문자 비율 계산
        korean_ratio = len(korean_pattern.findall(text)) / total_chars
        chinese_ratio = len(chinese_pattern.findall(text)) / total_chars
        japanese_ratio = len(japanese_pattern.findall(text)) / total_chars
        arabic_ratio = len(arabic_pattern.findall(text)) / total_chars
        cyrillic_ratio = len(cyrillic_pattern.findall(text)) / total_chars
        thai_ratio = len(thai_pattern.findall(text)) / total_chars
        
        # 언어 결정 (임계값 0.1 이상)
        if korean_ratio > 0.1:
            results.append("ko")
        elif arabic_ratio > 0.1:
            results.append("ar")  # 아랍어
        elif chinese_ratio > 0.1:
            results.append("zh")  # 중국어
        elif japanese_ratio > 0.1:
            results.append("ja")  # 일본어
        elif cyrillic_ratio > 0.1:
            results.append("ru")  # 러시아어
        elif thai_ratio > 0.1:
            results.append("th")  # 태국어
        else:
            results.append("en")  # 기본값: 영어
    
    # 언어별 통계 (로깅 제거)
    # lang_counts = pd.Series(results).value_counts().to_dict()
    return pd.Series(results)

@pandas_udf(StringType())
def analyze_sentiment_advanced(texts: pd.Series) -> pd.Series:
    """고도화된 감성 분석 UDF - 2단계 폴백 시스템"""
    
    try:
        # 텍스트 전처리
        clean_texts = []
        for text in texts:
            if pd.isna(text) or not isinstance(text, str):
                clean_texts.append("")
            else:
                # 길이 제한 및 정리
                clean_text = text.strip()[:1000]  # 1000자 제한
                clean_texts.append(clean_text)
        
        # 고급 폴백 시스템 사용 (HuggingFace → 키워드 기반)
        try:
            sentiment_labels = analyze_sentiment_batch_sync(clean_texts, language='auto')
            return pd.Series(sentiment_labels)
            
        except Exception as advanced_error:
            # 고급 분석 실패 시 간단한 키워드 기반 폴백
            sentiment_labels = []
            
            # 간단한 키워드 기반 감성 분석 (최종 폴백)
            positive_keywords = ['good', 'great', 'excellent', 'amazing', 'wonderful', 'love', 'best', 'perfect', 'awesome', '좋', '최고', '훌륭', '멋진', '완벽']
            negative_keywords = ['bad', 'terrible', 'awful', 'worst', 'hate', 'horrible', 'disgusting', '나쁘', '최악', '끔찍', '혐오']
            
            for text in clean_texts:
                if not text:
                    sentiment_labels.append("NEUTRAL")
                    continue
                    
                text_lower = text.lower()
                pos_count = sum(1 for word in positive_keywords if word in text_lower)
                neg_count = sum(1 for word in negative_keywords if word in text_lower)
                
                if pos_count > neg_count:
                    sentiment_labels.append("POSITIVE")
                elif neg_count > pos_count:
                    sentiment_labels.append("NEGATIVE")
                else:
                    sentiment_labels.append("NEUTRAL")
            
            return pd.Series(sentiment_labels)
            
    except Exception as e:
        # 감성 분석 UDF 전체 실패
        # 최종 폴백: 모두 NEUTRAL
        return pd.Series(["NEUTRAL"] * len(texts))

class SentimentAnalyzerV2:
    """개선된 감성 분석 시스템"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.spark = None
        # 고도화된 감성 분석기 초기화 (2단계 폴백 시스템)
        self.sentiment_analyzer = get_sentiment_analyzer()
        self.executor = ThreadPoolExecutor(max_workers=4)  # 비동기 처리용
        
        # 성능 메트릭
        self.metrics = {
            'processed_batches': 0,
            'total_records': 0,
            'api_calls': 0,
            'cache_hits': 0,
            'errors': 0
        }
        
        self.logger.info(f"{APP_NAME} 초기화 완료")
    
    def create_spark_session(self) -> SparkSession:
        """최적화된 SparkSession 생성"""
        try:
            spark = SparkSession.builder \
                .appName(APP_NAME) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
                .config("spark.hadoop.fs.defaultFS", settings.HDFS_RPC_URL) \
                .config("spark.sql.parquet.writeLegacyFormat", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.executor.memory", settings.SPARK_EXECUTOR_MEMORY) \
                .config("spark.driver.memory", settings.SPARK_DRIVER_MEMORY) \
                .config("spark.executor.cores", str(settings.SPARK_EXECUTOR_CORES)) \
                .getOrCreate()
            
            # 로깅 레벨 설정
            spark.sparkContext.setLogLevel("WARN")
            
            self.logger.info(f"SparkSession 생성 완료 - 메모리: {settings.SPARK_EXECUTOR_MEMORY}")
            return spark
            
        except Exception as e:
            self.logger.error(f"SparkSession 생성 실패: {e}", exc_info=True)
            return None
    
    def create_data_schema(self) -> StructType:
        """Reddit 데이터 스키마 정의"""
        return StructType([
            StructField("id", StringType()),
            StructField("title", StringType()),
            StructField("text", StringType()),
            StructField("created_utc", FloatType()),
            StructField("author_name", StringType()),
            StructField("type", StringType()),
            StructField("submission_id", StringType()),
            StructField("parent_id", StringType())
        ])
    
    def process_batch(self, batch_df, batch_id: int):
        """배치 처리 로직 (성능 최적화)"""
        try:
            start_time = time.time()
            initial_count = batch_df.count()
            
            self.logger.info(f"배치 {batch_id} 처리 시작: {initial_count}개 레코드")
            
            if initial_count == 0:
                self.logger.warning(f"배치 {batch_id}: 빈 배치, 처리 스킵")
                return
            
            # 텍스트 필터링 (강화된 조건)
            filtered_df = batch_df.filter(
                col("text").isNotNull() & 
                (length(trim(col("text"))) > 10) &  # 최소 10자
                ~col("text").contains("[deleted]") &
                ~col("text").contains("[removed]") &
                ~col("text").rlike("^[\\s\\W]*$")  # 공백/특수문자만 제외
            )
            
            filtered_count = filtered_df.count()
            self.logger.info(f"배치 {batch_id}: 필터링 후 {filtered_count}개 레코드")
            
            if filtered_count == 0:
                self.logger.warning(f"배치 {batch_id}: 필터링 후 빈 배치")
                return
            
            # 시간 변환 (한국 시간)
            time_converted_df = filtered_df.withColumn(
                "created_at",
                from_utc_timestamp(from_unixtime(col("created_utc")), "Asia/Seoul")
            )
            
            # 언어 감지
            lang_detected_df = time_converted_df.withColumn(
                "detected_language", 
                detect_language_optimized(col("text"))
            )
            
            # 고도화된 감성 분석
            final_df = lang_detected_df.withColumn(
                "sentiment_label",
                analyze_sentiment_advanced(col("text"))
            )
            
            # 최종 레코드 수 확인
            final_count = final_df.count()
            self.logger.info(f"배치 {batch_id}: 최종 {final_count}개 레코드 처리 완료")
            
            if final_count == 0:
                self.logger.warning(f"배치 {batch_id}: 최종 결과가 빈 배치")
                return
            
            # 원자적 쓰기 (임시 → 최종)
            timestamp = int(time.time())
            unique_id = str(uuid.uuid4())[:8]
            temp_path = f"{settings.SENTIMENT_DATA_PATH}_temp/batch_{batch_id}_{timestamp}_{unique_id}"
            final_path = f"{settings.SENTIMENT_DATA_PATH}/batch_{batch_id}_{timestamp}_{unique_id}.parquet"
            
            # 임시 위치에 쓰기
            final_df.write \
                .mode("overwrite") \
                .option("compression", settings.PARQUET_COMPRESSION) \
                .parquet(temp_path)
            
            # 원자적 이동 (HDFS)
            spark = final_df.sql_ctx.sparkSession
            hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
            fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
            
            temp_dir = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_path)
            final_dir = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(settings.SENTIMENT_DATA_PATH)
            
            # 최종 디렉토리 생성
            if not fs.exists(final_dir):
                fs.mkdirs(final_dir)
            
            # 파일 이동
            temp_files = fs.listStatus(temp_dir)
            for temp_file in temp_files:
                if temp_file.getPath().getName().endswith('.parquet'):
                    src_path = temp_file.getPath()
                    dst_path = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(
                        final_dir,
                        f"part-{batch_id}-{timestamp}-{unique_id}-{temp_file.getPath().getName()}"
                    )
                    fs.rename(src_path, dst_path)
            
            # 임시 디렉토리 삭제
            fs.delete(temp_dir, True)
            
            # 메트릭 업데이트
            self.metrics['processed_batches'] += 1
            self.metrics['total_records'] += final_count
            
            processing_time = time.time() - start_time
            self.logger.info(
                f"배치 {batch_id} 완료: {final_count}개 레코드, "
                f"처리시간 {processing_time:.2f}초"
            )
            
        except Exception as e:
            self.metrics['errors'] += 1
            self.logger.error(f"배치 {batch_id} 처리 오류: {e}", exc_info=True)
            raise
    
    def run_streaming_analysis(self):
        """실시간 스트리밍 감성 분석 실행"""
        max_retries = 5
        retry_delay_sec = 10
        
        for attempt in range(max_retries):
            try:
                self.logger.info(f"스트리밍 분석 시작 (시도 {attempt + 1})")
                
                # Kafka 스트림 읽기
                df = self.spark.readStream \
                    .format("kafka") \
                    .option("kafka.bootstrap.servers", settings.KAFKA_BROKER) \
                    .option("subscribe", settings.KAFKA_TOPIC) \
                    .option("failOnDataLoss", "false") \
                    .option("startingOffsets", "latest") \
                    .option("maxOffsetsPerTrigger", "1000") \
                    .load()
                
                # JSON 파싱
                schema = self.create_data_schema()
                parsed_df = df.select(
                    from_json(col("value").cast("string"), schema).alias("data")
                ).select("data.*")
                
                # 스트리밍 쿼리 시작
                query = parsed_df.writeStream \
                    .outputMode("append") \
                    .foreachBatch(self.process_batch) \
                    .option("checkpointLocation", f"{settings.SENTIMENT_DATA_PATH}/_checkpoints") \
                    .trigger(processingTime=f'{settings.SPARK_BATCH_INTERVAL_SECONDS} seconds') \
                    .start()
                
                self.logger.info(f"스트리밍 쿼리 시작됨: {settings.SENTIMENT_DATA_PATH}")
                
                # 메트릭 로깅 (5분마다)
                last_metric_log = time.time()
                while query.isActive:
                    time.sleep(10)  # 10초마다 상태 확인
                    
                    current_time = time.time()
                    if current_time - last_metric_log >= 300:  # 5분
                        self.logger.info(f"처리 메트릭: {self.metrics}")
                        # 간단한 키워드 기반 감성분석으로 변경되어 별도 통계 없음
                        last_metric_log = current_time
                
                query.awaitTermination()
                return  # 성공 시 함수 종료
                
            except Exception as e:
                self.logger.error(f"스트리밍 분석 오류 (시도 {attempt + 1}): {e}", exc_info=True)
                
                if "Connection refused" in str(e) or "ConnectException" in str(e):
                    self.logger.warning(f"연결 문제 감지. {retry_delay_sec}초 후 재시도...")
                    time.sleep(retry_delay_sec)
                    retry_delay_sec = min(retry_delay_sec * 2, 120)  # 최대 2분
                else:
                    self.logger.error("치명적 오류 발생. 재시도하지 않습니다.")
                    raise
        
        self.logger.error(f"최대 재시도 횟수({max_retries}) 초과")
        raise Exception("스트리밍 분석 시작 실패")
    
    def run(self):
        """메인 실행 함수"""
        try:
            self.logger.info(f"{APP_NAME} 시작")
            
            # SparkSession 생성
            self.spark = self.create_spark_session()
            if not self.spark:
                raise Exception("SparkSession 생성 실패")
            
            # 스트리밍 분석 실행
            self.run_streaming_analysis()
            
        except KeyboardInterrupt:
            self.logger.info("사용자 중단 요청")
        except Exception as e:
            self.logger.error(f"실행 중 오류: {e}", exc_info=True)
            raise
        finally:
            if self.spark:
                self.logger.info("SparkSession 정리 중...")
                self.spark.stop()
            if self.executor:
                self.executor.shutdown(wait=True)
            self.logger.info(f"{APP_NAME} 종료")

def main():
    """엔트리 포인트"""
    analyzer = SentimentAnalyzerV2()
    analyzer.run()

if __name__ == "__main__":
    main()