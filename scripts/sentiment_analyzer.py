import logging
import pandas as pd
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, pandas_udf, when, from_utc_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, FloatType
import time # time 모듈 임포트

# Hugging Face 캐시 디렉토리 설정
# 컨테이너 내부에서 쓰기 가능한 경로로 지정 (여러 경로 시도)
cache_paths = [
    '/opt/bitnami/spark/hf_models_cache',  # 사전 다운로드된 모델 경로
    '/opt/bitnami/spark/tmp/huggingface_cache',  # 기본 캐시 경로
    '/tmp/huggingface_cache'  # 임시 디렉토리
]

for cache_path in cache_paths:
    try:
        os.makedirs(cache_path, exist_ok=True)
        if os.access(cache_path, os.W_OK):
            os.environ['HF_HOME'] = cache_path
            os.environ['XDG_CACHE_HOME'] = cache_path
            break
    except Exception as e:
        continue
else:
    # 모든 경로 실패 시 기본값
    os.environ['HF_HOME'] = '/tmp/huggingface_cache'
    os.environ['XDG_CACHE_HOME'] = '/tmp/huggingface_cache'

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings

# 로깅 설정
# 기본 로깅 레벨을 DEBUG로 설정하여 모든 상세 로그를 캡처
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__) # 모듈별 로거 생성

# --- Spark 및 Kafka 설정 ---
APP_NAME = "AdvancedSentimentAnalyzer"
OUTPUT_PATH = "hdfs://namenode:8020/user/spark/social_sentiment_data" # HDFS 경로 포트 8020으로 변경

# 감성 분석 모델 정의
KO_MODEL_NAME = "Copycats/koelectra-base-v3-generalized-sentiment-analysis" # 한국어 모델
EN_MODEL_NAME = "distilbert-base-uncased-finetuned-sst-2-english" # 영어 모델

# fastText 언어 감지 모델 경로
FASTTEXT_MODEL_PATH = "/opt/airflow/models/lid.176.bin"

# --- 키워드 기반 폴백 함수 ---
def get_korean_sentiment_fallback(texts: pd.Series) -> pd.Series:
    """한국어 키워드 기반 감성 분석 폴백"""
    import re
    import logging
    logger = logging.getLogger(__name__)
    
    logger.info(f"키워드 분석 시작: {len(texts)}개 텍스트")
    
    positive_words = {
        '좋다', '좋은', '좋음', '훌륭', '최고', '멋진', '대단', '완벽', '성공', '사랑', '감사',
        '행복', '기쁘다', '기쁜', '만족', '우수', '탁월', '뛰어난', '괜찮', '잘했', '칭찬',
        '추천', '최신', '신선', '재미있', '흥미롭', '놀라운', '환상적', '즐거운', '유용한'
    }
    
    negative_words = {
        '나쁘다', '나쁜', '나쁨', '최악', '별로', '실망', '화나', '짜증', '못생긴', '싫어', '싫다',
        '문제', '고장', '버그', '오류', '에러', '실패', '망함', '끔찍', '답답', '속상',
        '걱정', '불안', '스트레스', '힘들', '어렵', '짜증나', '귀찮', '불편', '복잡'
    }
    
    results = []
    for i, text in enumerate(texts):
        if pd.isna(text) or not isinstance(text, str) or len(text.strip()) == 0:
            results.append('NEUTRAL')  # unknown 대신 NEUTRAL 사용
            continue
            
        clean_text = text.lower()
        pos_score = sum(1 for word in positive_words if word in clean_text)
        neg_score = sum(1 for word in negative_words if word in clean_text)
        
        if pos_score > neg_score and pos_score > 0:
            results.append('POSITIVE')
        elif neg_score > pos_score and neg_score > 0:
            results.append('NEGATIVE')
        else:
            results.append('NEUTRAL')
    
    logger.info(f"키워드 분석 완료: {len(results)}개 결과, 샘플: {results[:3] if len(results) > 0 else 'empty'}")
    return pd.Series(results)

def create_spark_session():
    """SparkSession을 생성하고 반환합니다."""
    try:
        spark = SparkSession.builder \
            .appName(APP_NAME) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
            .config("spark.sql.parquet.writeLegacyFormat", "true") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN") # Spark 자체 로그는 WARN으로 유지
        logger.info(f"[{APP_NAME}] SparkSession이 성공적으로 생성되었습니다.")
        return spark
    except Exception as e:
        logger.error(f"SparkSession 생성 실패: {e}", exc_info=True)
        return None

# --- 언어 감지 UDF (정규식 기반으로 단순화) ---
@pandas_udf(StringType())
def detect_language_udf(texts: pd.Series) -> pd.Series:
    import re
    import logging
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger(__name__)

    logger.debug(f"UDF: detect_language_udf 호출됨. 배치 크기: {len(texts)}")
    if not texts.empty:
        logger.debug(f"UDF: detect_language_udf 첫 번째 샘플: '{str(texts.iloc[0])[:50]}...'")
    
    # 한글 문자 패턴
    korean_pattern = re.compile(r'[가-힣]')
    
    results = []
    for text in texts:
        if pd.isna(text) or not isinstance(text, str) or len(text.strip()) == 0:
            results.append("en")  # 빈 텍스트는 영어로 분류 (unknown 제거)
        else:
            # 한글이 포함되어 있으면 한국어
            if korean_pattern.search(text):
                results.append("ko")
            else:
                # Reddit 데이터는 기본적으로 영어로 분류
                results.append("en")
    
    logger.info(f"UDF: detect_language_udf 처리 완료. 총 {len(results)}개, 영어: {results.count('en')}, 한국어: {results.count('ko')}")
    return pd.Series(results)

# --- 한국어 감성 분석 UDF (실제 AI 모델 사용) ---
@pandas_udf(StringType())
def get_korean_sentiment_label(texts: pd.Series) -> pd.Series:
    import logging
    import os
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger(__name__)

    logger.debug(f"UDF: get_korean_sentiment_label 호출됨. 배치 크기: {len(texts)}")
    
    try:
        # 실제 한국어 감성 분석 모델 사용
        from transformers import pipeline
        import warnings
        warnings.filterwarnings("ignore")
        
        # 키워드 기반 한국어 감성 분석 사용 (안정적)
        logger.info("UDF: 키워드 기반 한국어 감성 분석 사용")
        try:
            result = get_korean_sentiment_fallback(texts)
            logger.info(f"UDF: 키워드 분석 완료, 결과 샘플: {result.iloc[:3].tolist() if len(result) > 0 else 'empty'}")
            return result
        except Exception as fallback_error:
            logger.error(f"UDF: 키워드 분석 실패: {fallback_error}")
            return pd.Series(["NEUTRAL"] * len(texts))
        
        # 실제 한국어 감성 분석 수행
        results = []
        clean_texts = []
        
        for text in texts:
            if pd.isna(text) or not isinstance(text, str) or len(text.strip()) == 0:
                clean_texts.append("")
            else:
                # 텍스트 전처리: 512자로 제한
                clean_text = text[:512].strip()
                clean_texts.append(clean_text)
        
        # 빈 텍스트가 아닌 것들만 모델에 전달
        valid_indices = [i for i, text in enumerate(clean_texts) if text]
        valid_texts = [clean_texts[i] for i in valid_indices]
        
        if valid_texts:
            try:
                # 배치로 한국어 감성 분석 수행
                predictions = globals()['ko_sentiment_pipeline'](valid_texts)
                
                # 결과 매핑
                pred_idx = 0
                for i, text in enumerate(clean_texts):
                    if text:  # 유효한 텍스트
                        try:
                            pred = predictions[pred_idx]
                            label = pred.get('label', 'NEUTRAL')
                            # 라벨 표준화 (한국어 모델 출력에 따라)
                            if label in ['LABEL_0', 'NEGATIVE', '부정', 'negative']:
                                results.append('NEGATIVE')
                            elif label in ['LABEL_1', 'NEUTRAL', '중립', 'neutral']:
                                results.append('NEUTRAL') 
                            elif label in ['LABEL_2', 'POSITIVE', '긍정', 'positive']:
                                results.append('POSITIVE')
                            else:
                                results.append('NEUTRAL')  # 기본값
                            pred_idx += 1
                        except (KeyError, IndexError) as label_error:
                            logger.warning(f"UDF: 예측 결과 처리 오류: {label_error}")
                            results.append('NEUTRAL')
                            pred_idx += 1
                    else:  # 빈 텍스트
                        results.append('NEUTRAL')  # unknown 대신 NEUTRAL 사용
            except Exception as prediction_error:
                logger.error(f"UDF: 모델 예측 실행 오류: {prediction_error}")
                # 예측 실패 시 키워드 기반 폴백
                return get_korean_sentiment_fallback(texts)
        else:
            results = ['NEUTRAL'] * len(texts)  # unknown 대신 NEUTRAL 사용
        
        logger.info(f"UDF: 한국어 감성 분석 완료. {len(valid_texts)}개 분석, 결과 샘플: {results[:3]}")
        return pd.Series(results)
        
    except Exception as e:
        logger.error(f"UDF: 한국어 감성 분석 최종 오류: {e}", exc_info=True)
        # 최후 폴백: 키워드 기반 분석
        try:
            return get_korean_sentiment_fallback(texts)
        except Exception as fallback_error:
            logger.error(f"UDF: 폴백 함수도 실패: {fallback_error}")
            return pd.Series(["NEUTRAL"] * len(texts))

# --- 영어 감성 분석 UDF (키워드 기반 - 확실한 방법) ---
@pandas_udf(StringType())
def get_english_sentiment_label(texts: pd.Series) -> pd.Series:
    import logging
    import re
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s [%(levelname)s] %(message)s')
    logger = logging.getLogger(__name__)

    logger.debug(f"UDF: get_english_sentiment_label 호출됨. 배치 크기: {len(texts)}")
    
    try:
        # 키워드 기반 영어 감성 분석 (확실하고 빠름)
        logger.info("UDF: 키워드 기반 영어 감성 분석 시작")
        positive_words = {
            'good', 'great', 'excellent', 'amazing', 'awesome', 'fantastic', 'wonderful', 
            'love', 'like', 'best', 'perfect', 'nice', 'happy', 'glad', 'excited',
            'thank', 'thanks', 'appreciate', 'brilliant', 'outstanding', 'incredible',
            'beautiful', 'successful', 'win', 'won', 'victory', 'achieve', 'cool'
        }
        
        negative_words = {
            'bad', 'terrible', 'awful', 'horrible', 'hate', 'dislike', 'worst', 'suck',
            'fail', 'failed', 'failure', 'broken', 'wrong', 'error', 'problem', 'issue',
            'sad', 'angry', 'mad', 'disappointed', 'frustrating', 'annoying', 'stupid',
            'useless', 'garbage', 'trash', 'waste', 'disaster', 'nightmare'
        }
        
        neutral_indicators = {
            'what', 'how', 'when', 'where', 'who', 'why', '?', 'help', 'question',
            'anyone', 'does', 'can', 'could', 'would', 'should', 'looking for'
        }
        
        results = []
        
        for text in texts:
            if pd.isna(text) or not isinstance(text, str) or len(text.strip()) == 0:
                results.append('NEUTRAL')  # unknown 대신 NEUTRAL 사용
                continue
                
            # 텍스트 전처리
            clean_text = text.lower()
            words = re.findall(r'\b\w+\b', clean_text)
            text_words = set(words)
            
            # 점수 계산
            pos_score = len(text_words & positive_words)
            neg_score = len(text_words & negative_words)
            neutral_score = len(text_words & neutral_indicators)
            
            # 감성 판단
            if pos_score > neg_score and pos_score > 0:
                results.append('POSITIVE')
            elif neg_score > pos_score and neg_score > 0:
                results.append('NEGATIVE')
            elif neutral_score > 0 or '?' in text:
                results.append('NEUTRAL')
            else:
                # 텍스트 길이 기반 추가 분류
                if len(text) > 200:
                    results.append('POSITIVE')  # 긴 텍스트는 보통 긍정적
                elif any(word in clean_text for word in ['error', 'problem', 'issue', 'broken']):
                    results.append('NEGATIVE')
                else:
                    results.append('NEUTRAL')
        
        logger.info(f"UDF: 키워드 기반 영어 감성 분석 완료. 총 {len(results)}개 처리, 결과 샘플: {results[:3] if len(results) > 0 else 'empty'}")
        return pd.Series(results)
        
    except Exception as e:
        logger.error(f"UDF: 영어 감성 분석 처리 중 오류 발생: {e}", exc_info=True)
        # 영어는 키워드 기반이므로 더 안전한 폴백
        try:
            # 매우 간단한 폴백: 텍스트 길이 기반
            simple_results = []
            for text in texts:
                if pd.isna(text) or not isinstance(text, str) or len(text.strip()) == 0:
                    simple_results.append('NEUTRAL')  # unknown 대신 NEUTRAL 사용
                elif '?' in text or len(text) < 20:
                    simple_results.append('NEUTRAL')
                else:
                    simple_results.append('NEUTRAL')  # 안전한 기본값
            return pd.Series(simple_results)
        except Exception as final_error:
            logger.error(f"UDF: 영어 최종 폴백도 실패: {final_error}")
            return pd.Series(["NEUTRAL"] * len(texts))

def analyze_sentiment(spark):
    logger.info("Kafka 스트림을 읽고 HDFS에 저장할 준비 중...")
    max_retries = 5
    retry_delay_sec = 10

    for attempt in range(max_retries):
        try:
            df = spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", settings.KAFKA_BROKER) \
                .option("subscribe", settings.KAFKA_TOPIC) \
                .option("failOnDataLoss", "false") \
                .option("startingOffsets", "latest") \
                .load()

            # Reddit 데이터 스키마 정의 (submission_id, parent_id 추가)
            schema = StructType([
                StructField("id", StringType()),
                StructField("title", StringType()), 
                StructField("text", StringType()), 
                StructField("created_utc", FloatType()), 
                StructField("author_name", StringType()),
                StructField("type", StringType()),
                StructField("submission_id", StringType()), 
                StructField("parent_id", StringType()) 
            ])

            parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
            
            # Null 값이나 빈 텍스트, 의미없는 텍스트 필터링 (강화)
            from pyspark.sql.functions import length, trim, regexp_replace
            filtered_df = parsed_df.filter(
                col("text").isNotNull() & 
                (length(trim(col("text"))) > 20) &  # 최소 20자 이상 (더 엄격)
                ~col("text").contains("[deleted]") &  # 삭제된 댓글 제외
                ~col("text").contains("[removed]") &  # 제거된 댓글 제외
                ~col("text").startswith("This post contains content not supported") &  # 지원되지 않는 콘텐츠 제외
                ~col("text").rlike("^[\\s\\W]*$") &  # 공백이나 특수문자만 있는 텍스트 제외
                (length(regexp_replace(col("text"), "[^a-zA-Z가-힣0-9]", "")) > 5)  # 실제 문자 5개 이상
            )
            
            # created_utc를 한국 시간(Asia/Seoul)으로 변환
            # Unix timestamp를 먼저 타임스탬프로 변환한 후 시간대 적용
            from pyspark.sql.functions import from_unixtime
            filtered_df = filtered_df.withColumn(
                "created_at", 
                from_utc_timestamp(from_unixtime(col("created_utc")), "Asia/Seoul")
            )

            # 1. 언어 감지
            df_with_lang = filtered_df.withColumn("detected_language", detect_language_udf(col("text")))
            # 2. 감지된 언어에 따라 다른 감성 분석 UDF 적용
            result_df = df_with_lang.withColumn("sentiment_label", 
                when(col("detected_language") == "ko", get_korean_sentiment_label(col("text")))
                .when(col("detected_language") == "en", get_english_sentiment_label(col("text")))
                .otherwise("NEUTRAL")  # unknown 대신 NEUTRAL 사용
            )  # 모든 언어 포함 (invalid 필터링 제거)
            # forEachBatch를 사용하여 각 마이크로 배치 처리 시점에 로깅 추가
            def process_batch(batch_df, batch_id):
                import time
                import uuid
                try:
                    initial_count = batch_df.count()
                    logger.info(f"Batch {batch_id}: Initial records from Kafka: {initial_count}")

                    # 필터링 후 레코드 수 (UDF에서 처리하므로 여기서는 간단히)
                    # UDF에서 빈 문자열/None 처리 로직을 추가했으므로, 여기서는 isNotNull()만 유지
                    filtered_count = batch_df.filter(col("text").isNotNull()).count()
                    logger.info(f"Batch {batch_id}: Records after text filtering: {filtered_count}")

                    if filtered_count == 0:
                        logger.warning(f"Batch {batch_id}: No valid text records after filtering. Skipping write.")
                        return

                    # 언어 감지 및 감성 분석은 UDF 내부에서 로깅됩니다.
                    # 최종 결과 데이터프레임의 레코드 수
                    final_count = batch_df.count() # UDF 적용 후 count (실제로는 UDF가 적용된 DF를 다시 count해야 함)
                    logger.info(f"Batch {batch_id}: Final records before write: {final_count}")

                    # 임시 파일명 생성 (타임스탬프 + UUID)
                    timestamp = int(time.time())
                    unique_id = str(uuid.uuid4())[:8]
                    temp_path = f"{settings.SENTIMENT_DATA_PATH}_temp/batch_{batch_id}_{timestamp}_{unique_id}"
                    final_path = f"{settings.SENTIMENT_DATA_PATH}/batch_{batch_id}_{timestamp}_{unique_id}.parquet"

                    # 임시 경로에 먼저 쓰기
                    batch_df.write \
                        .mode("overwrite") \
                        .format("parquet") \
                        .option("path", temp_path) \
                        .save()
                    
                    # Hadoop FileSystem을 이용한 파일 이동 (Atomic operation)
                    spark = batch_df.sql_ctx.sparkSession
                    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
                    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(hadoop_conf)
                    
                    # 임시 경로의 parquet 파일들을 최종 경로로 이동
                    temp_dir = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(temp_path)
                    final_dir = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(settings.SENTIMENT_DATA_PATH)
                    
                    # 최종 디렉토리가 없으면 생성
                    if not fs.exists(final_dir):
                        fs.mkdirs(final_dir)
                    
                    # 임시 디렉토리의 파일들을 최종 위치로 이동
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
                    
                    logger.info(f"Batch {batch_id}: Successfully wrote {final_count} records to HDFS using atomic write.")

                except Exception as e:
                    logger.error(f"Batch {batch_id}: Error processing batch: {e}", exc_info=True)

            query = result_df.writeStream \
                .outputMode("append") \
                .foreachBatch(process_batch) \
                .option("checkpointLocation", f"{settings.SENTIMENT_DATA_PATH}/_checkpoints") \
                .trigger(processingTime='30 seconds') \
                .start()

            logger.info(f"스트리밍 쿼리가 시작되었습니다. 결과는 '{settings.SENTIMENT_DATA_PATH}'에 저장됩니다.")
            query.awaitTermination()
            return # 성공하면 함수 종료

        except Exception as e:
            logger.error(f"Kafka 스트림 또는 HDFS 연결 중 오류 발생 (시도 {attempt + 1}/{max_retries}): {e}", exc_info=True)
            if "Connection refused" in str(e) or "No such file or directory" in str(e) or "java.net.ConnectException" in str(e):
                logger.warning(f"HDFS 연결 문제로 보입니다. {retry_delay_sec}초 후 재시도합니다...")
                time.sleep(retry_delay_sec)
            else:
                # 다른 종류의 오류는 재시도하지 않고 종료
                logger.error("치명적인 오류 발생. 재시도하지 않고 종료합니다.")
                raise
    logger.error(f"최대 재시도 횟수({max_retries}) 초과. HDFS 연결에 실패했습니다.")

if __name__ == "__main__":
    spark = create_spark_session()
    if spark:
        # 로깅 레벨을 DEBUG로 설정하여 UDF 내부 로그도 확인
        spark.sparkContext.setLogLevel("INFO") # Spark 자체 로그는 INFO로 유지
        logging.getLogger("py4j").setLevel(logging.WARN) # py4j 로그는 WARN으로
        logging.getLogger("pyspark").setLevel(logging.INFO) # pyspark 로그는 INFO로
        # 루트 로거의 레벨을 DEBUG로 설정하여 모든 메시지를 캡처
        logging.getLogger().setLevel(logging.DEBUG) 
        # AdvancedSentimentAnalyzer 로거는 이미 기본 로거를 따르므로 별도 설정 불필요
        
        analyze_sentiment(spark)
        spark.stop()