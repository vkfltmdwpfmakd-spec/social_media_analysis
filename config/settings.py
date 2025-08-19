"""
중앙 설정 파일 - 보안 강화 버전
- 환경변수 기반 설정으로 보안성 향상
- 환경별 설정 분리 (development, production)
- 민감한 정보는 환경변수로 관리
"""

import os
from typing import Optional

class SecurityConfig:
    """보안 관련 설정 관리"""
    
    @staticmethod
    def get_environment() -> str:
        """현재 실행 환경 반환"""
        return os.getenv('ENVIRONMENT', 'development').lower()
    
    @staticmethod
    def is_production() -> bool:
        """프로덕션 환경 여부 확인"""
        return SecurityConfig.get_environment() == 'production'
    
    @staticmethod
    def get_secure_value(key: str, default: Optional[str] = None, required: bool = False) -> str:
        """환경변수에서 안전하게 값 가져오기"""
        value = os.getenv(key, default)
        if required and not value:
            raise ValueError(f"필수 환경변수 {key}가 설정되지 않았습니다.")
        return value

# --- 환경 설정 ---
ENVIRONMENT = SecurityConfig.get_environment()
IS_PRODUCTION = SecurityConfig.is_production()

# --- Kafka 설정 (보안 강화) ---
KAFKA_BROKER = SecurityConfig.get_secure_value(
    'KAFKA_BROKER', 
    'kafka:9092' if not IS_PRODUCTION else None,
    required=IS_PRODUCTION
)
KAFKA_TOPIC = SecurityConfig.get_secure_value('KAFKA_TOPIC', 'reddit-stream')

# --- HDFS 설정 (환경별 분리) ---
if IS_PRODUCTION:
    # 프로덕션: 환경변수 필수
    HDFS_NAMENODE_HOST = SecurityConfig.get_secure_value('HDFS_NAMENODE_HOST', required=True)
    HDFS_NAMENODE_PORT = int(SecurityConfig.get_secure_value('HDFS_NAMENODE_PORT', '8020'))
    HDFS_WEB_PORT = int(SecurityConfig.get_secure_value('HDFS_WEB_PORT', '9870'))
else:
    # 개발환경: 기본값 사용
    HDFS_NAMENODE_HOST = SecurityConfig.get_secure_value('HDFS_NAMENODE_HOST', 'namenode')
    HDFS_NAMENODE_PORT = int(SecurityConfig.get_secure_value('HDFS_NAMENODE_PORT', '8020'))
    HDFS_WEB_PORT = int(SecurityConfig.get_secure_value('HDFS_WEB_PORT', '9870'))

# HDFS URL 구성
HDFS_RPC_URL = f"hdfs://{HDFS_NAMENODE_HOST}:{HDFS_NAMENODE_PORT}"
HDFS_WEB_URL = f"http://{HDFS_NAMENODE_HOST}:{HDFS_WEB_PORT}"

# 데이터 저장 경로
HDFS_BASE_PATH = f"{HDFS_RPC_URL}/user/spark"
SENTIMENT_DATA_PATH = f"{HDFS_BASE_PATH}/social_sentiment_data"
DAILY_REPORT_PATH = f"{HDFS_BASE_PATH}/daily_sentiment_reports"

# --- Spark 설정 (성능 최적화) ---
SPARK_MASTER_HOST = SecurityConfig.get_secure_value('SPARK_MASTER_HOST', 'spark-master')
SPARK_MASTER_PORT = int(SecurityConfig.get_secure_value('SPARK_MASTER_PORT', '7077'))
SPARK_MASTER_URL = f"spark://{SPARK_MASTER_HOST}:{SPARK_MASTER_PORT}"

# Spark 메모리 설정 (환경별)
if IS_PRODUCTION:
    SPARK_EXECUTOR_MEMORY = SecurityConfig.get_secure_value('SPARK_EXECUTOR_MEMORY', '4g')
    SPARK_DRIVER_MEMORY = SecurityConfig.get_secure_value('SPARK_DRIVER_MEMORY', '2g')
    SPARK_EXECUTOR_CORES = int(SecurityConfig.get_secure_value('SPARK_EXECUTOR_CORES', '4'))
else:
    SPARK_EXECUTOR_MEMORY = SecurityConfig.get_secure_value('SPARK_EXECUTOR_MEMORY', '2g')
    SPARK_DRIVER_MEMORY = SecurityConfig.get_secure_value('SPARK_DRIVER_MEMORY', '1g')
    SPARK_EXECUTOR_CORES = int(SecurityConfig.get_secure_value('SPARK_EXECUTOR_CORES', '2'))

# --- 감성 분석 API 설정 (고도화) ---
# OpenAI API 설정
OPENAI_API_KEY = SecurityConfig.get_secure_value('OPENAI_API_KEY')
OPENAI_MODEL = SecurityConfig.get_secure_value('OPENAI_MODEL', 'gpt-3.5-turbo')

# Hugging Face API 설정
HF_API_KEY = SecurityConfig.get_secure_value('HUGGINGFACE_API_KEY')
HF_API_URL = SecurityConfig.get_secure_value('HF_API_URL', 'https://api-inference.huggingface.co/models')

# 기존 모델 (폴백용)
KO_SENTIMENT_MODEL = SecurityConfig.get_secure_value(
    'KO_SENTIMENT_MODEL', 
    'matthewde-v1/koelectra-base-v3-sentiment-5-class'
)
EN_SENTIMENT_MODEL = SecurityConfig.get_secure_value(
    'EN_SENTIMENT_MODEL', 
    'distilbert-base-uncased-finetuned-sst-2-english'
)

# 언어 감지 모델
FASTTEXT_MODEL_PATH = SecurityConfig.get_secure_value(
    'FASTTEXT_MODEL_PATH',
    '/opt/bitnami/spark/models/lid.176.bin'
)

# --- 감성 분석 품질 설정 ---
# API 우선순위 (API 우선 사용)
SENTIMENT_API_PRIORITY = [
    'huggingface', # 1순위: Hugging Face API
    'openai',      # 2순위: OpenAI GPT (비용 문제로 우선순위 낮춤)
    'local'        # 3순위: 폴백용 로컬 키워드 분석
]

# API 요청 제한
MAX_API_REQUESTS_PER_MINUTE = int(SecurityConfig.get_secure_value('MAX_API_REQUESTS_PER_MINUTE', '50'))
API_TIMEOUT_SECONDS = int(SecurityConfig.get_secure_value('API_TIMEOUT_SECONDS', '10'))

# --- 데이터 수집 설정 ---
SUBREDDIT_NAME = SecurityConfig.get_secure_value('SUBREDDIT_NAME', 'all')

# Reddit API 설정 (보안 강화)
# 대시보드는 Reddit API가 필요하지 않으므로 개발 환경에서는 선택적으로 설정
REDDIT_CLIENT_ID = SecurityConfig.get_secure_value(
    'REDDIT_CLIENT_ID', 
    default='dev_client_id' if not IS_PRODUCTION else None,
    required=IS_PRODUCTION
)
REDDIT_CLIENT_SECRET = SecurityConfig.get_secure_value(
    'REDDIT_CLIENT_SECRET', 
    default='dev_client_secret' if not IS_PRODUCTION else None,
    required=IS_PRODUCTION
)
REDDIT_USERNAME = SecurityConfig.get_secure_value(
    'REDDIT_USERNAME', 
    default='dev_username' if not IS_PRODUCTION else None,
    required=IS_PRODUCTION
)
REDDIT_PASSWORD = SecurityConfig.get_secure_value(
    'REDDIT_PASSWORD', 
    default='dev_password' if not IS_PRODUCTION else None,
    required=IS_PRODUCTION
)
REDDIT_USER_AGENT = SecurityConfig.get_secure_value(
    'REDDIT_USER_AGENT', 
    'social_media_analysis:v2.0 (by /u/dev_user)'
)

# --- 성능 튜닝 설정 ---
# 배치 처리 크기
SPARK_BATCH_INTERVAL_SECONDS = int(SecurityConfig.get_secure_value('SPARK_BATCH_INTERVAL', '30'))
MAX_FILES_PER_LOAD = int(SecurityConfig.get_secure_value('MAX_FILES_PER_LOAD', '10'))
PARQUET_COMPRESSION = SecurityConfig.get_secure_value('PARQUET_COMPRESSION', 'snappy')

# 대시보드 새로고침 간격
DASHBOARD_REFRESH_SECONDS = int(SecurityConfig.get_secure_value('DASHBOARD_REFRESH_SECONDS', '30'))

# --- 로깅 설정 ---
LOG_LEVEL = SecurityConfig.get_secure_value(
    'LOG_LEVEL',
    'INFO' if IS_PRODUCTION else 'DEBUG'
)

# 보안 로깅 (프로덕션에서 민감 정보 로깅 방지)
ENABLE_DEBUG_LOGGING = not IS_PRODUCTION
LOG_FILE_PATH = SecurityConfig.get_secure_value('LOG_FILE_PATH', '/var/log/social_media_analysis')

# --- 모니터링 설정 ---
HEALTH_CHECK_INTERVAL_SECONDS = int(SecurityConfig.get_secure_value('HEALTH_CHECK_INTERVAL', '60'))
ALERT_EMAIL = SecurityConfig.get_secure_value('ALERT_EMAIL')

# 설정 검증
def validate_config():
    """설정 검증 함수 - 컴포넌트별 선택적 검증"""
    errors = []
    
    # 현재 실행 중인 컴포넌트 감지
    import sys
    current_script = sys.argv[0] if sys.argv else ""
    is_reddit_producer = 'reddit_producer' in current_script
    is_dashboard = 'dashboard' in current_script or 'streamlit' in current_script
    
    if IS_PRODUCTION:
        # 프로덕션 환경에서는 모든 설정 필수
        required_vars = ['KAFKA_BROKER', 'HDFS_NAMENODE_HOST']
        
        # Reddit Producer만 Reddit API 필수
        if is_reddit_producer:
            required_vars.extend(['REDDIT_CLIENT_ID', 'REDDIT_CLIENT_SECRET'])
        
        for var in required_vars:
            value = globals().get(var)
            if not value or (isinstance(value, str) and value.startswith('dev_')):
                errors.append(f"프로덕션 환경에서 {var}는 실제 값이 필요합니다.")
    else:
        # 개발 환경에서는 관대한 검증
        if is_reddit_producer:
            # Reddit Producer는 실제 API 키가 필요함을 경고
            reddit_vars = [REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USERNAME, REDDIT_PASSWORD]
            if any(var.startswith('dev_') or var in ['your_', 'dummy_'] for var in reddit_vars if isinstance(var, str)):
                import logging
                logger = logging.getLogger(__name__)
                logger.warning("⚠️  Reddit Producer: 실제 Reddit API 키가 설정되지 않았습니다. .env 파일에서 설정하세요.")
    
    if errors:
        raise ValueError(f"설정 오류: {', '.join(errors)}")

# 모듈 로드 시 설정 검증 (에러가 아닌 경고로 처리)
try:
    validate_config()
except ValueError as e:
    # 개발 환경에서는 경고만 출력하고 계속 진행
    if not IS_PRODUCTION:
        import logging
        logging.warning(f"설정 경고: {e}")
    else:
        raise
