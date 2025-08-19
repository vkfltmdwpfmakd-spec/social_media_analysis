"""
중앙 집중식 로깅 설정 모듈
- 프로젝트 전반에서 일관된 로깅 설정을 제공합니다.
- 환경별 로그 레벨 및 포맷 관리
- 중복 코드 제거 및 유지보수성 향상
"""

import logging
import os
import sys
from datetime import datetime
from typing import Optional

class LoggingConfig:
    """로깅 설정 관리 클래스"""
    
    # 환경별 기본 로그 레벨
    LOG_LEVELS = {
        'development': logging.DEBUG,
        'testing': logging.INFO,
        'production': logging.WARNING
    }
    
    # 보안을 위한 민감 정보 필터링
    SENSITIVE_PATTERNS = [
        'password', 'secret', 'key', 'token', 'credential'
    ]
    
    @classmethod
    def get_environment(cls) -> str:
        """현재 환경 감지"""
        return os.getenv('ENVIRONMENT', 'development').lower()
    
    @classmethod
    def get_log_level(cls, custom_level: Optional[str] = None) -> int:
        """환경에 맞는 로그 레벨 반환"""
        if custom_level:
            return getattr(logging, custom_level.upper(), logging.INFO)
        
        env = cls.get_environment()
        return cls.LOG_LEVELS.get(env, logging.INFO)
    
    @classmethod
    def create_formatter(cls, include_module: bool = True) -> logging.Formatter:
        """로그 포맷터 생성"""
        if include_module:
            format_string = '%(asctime)s [%(levelname)s] %(name)s:%(lineno)d - %(message)s'
        else:
            format_string = '%(asctime)s [%(levelname)s] %(message)s'
        
        return logging.Formatter(
            format_string,
            datefmt='%Y-%m-%d %H:%M:%S'
        )
    
    @classmethod
    def setup_logger(cls, 
                    name: str, 
                    level: Optional[str] = None,
                    log_file: Optional[str] = None,
                    console_output: bool = True) -> logging.Logger:
        """
        로거 설정 및 반환
        
        Args:
            name: 로거 이름 (보통 __name__ 사용)
            level: 로그 레벨 (DEBUG, INFO, WARNING, ERROR, CRITICAL)
            log_file: 로그 파일 경로 (선택적)
            console_output: 콘솔 출력 여부
        
        Returns:
            설정된 로거 인스턴스
        """
        logger = logging.getLogger(name)
        
        # 이미 핸들러가 설정되어 있으면 중복 설정 방지
        if logger.handlers:
            return logger
        
        # 로그 레벨 설정
        log_level = cls.get_log_level(level)
        logger.setLevel(log_level)
        
        # 포맷터 생성
        formatter = cls.create_formatter()
        
        # 콘솔 핸들러 추가
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        # 파일 핸들러 추가 (선택적)
        if log_file:
            try:
                # 로그 디렉토리 생성
                log_dir = os.path.dirname(log_file)
                if log_dir and not os.path.exists(log_dir):
                    os.makedirs(log_dir, exist_ok=True)
                
                file_handler = logging.FileHandler(log_file, encoding='utf-8')
                file_handler.setLevel(log_level)
                file_handler.setFormatter(formatter)
                logger.addHandler(file_handler)
                
            except Exception as e:
                logger.warning(f"로그 파일 설정 실패: {e}")
        
        # 상위 로거로의 전파 방지 (중복 출력 방지)
        logger.propagate = False
        
        return logger
    
    @classmethod
    def filter_sensitive_info(cls, message: str) -> str:
        """민감한 정보 필터링"""
        filtered_message = message
        for pattern in cls.SENSITIVE_PATTERNS:
            if pattern.lower() in message.lower():
                # 민감한 정보가 포함된 경우 마스킹
                filtered_message = f"[로그에 민감한 정보 포함됨 - {pattern}]"
                break
        return filtered_message
    
    @classmethod
    def configure_spark_logging(cls):
        """Spark 관련 로깅 최적화"""
        # Spark의 과도한 로깅 억제
        logging.getLogger("py4j").setLevel(logging.WARNING)
        logging.getLogger("pyspark").setLevel(logging.INFO)
        logging.getLogger("org.apache.spark").setLevel(logging.WARNING)
        logging.getLogger("org.apache.hadoop").setLevel(logging.WARNING)

# 편의 함수들
def get_logger(name: str, 
               level: Optional[str] = None,
               log_file: Optional[str] = None) -> logging.Logger:
    """간편한 로거 생성 함수"""
    return LoggingConfig.setup_logger(name, level, log_file)

def setup_project_logging():
    """프로젝트 전체 로깅 초기 설정"""
    # 루트 로거 설정
    root_logger = logging.getLogger()
    root_logger.setLevel(LoggingConfig.get_log_level())
    
    # Spark 로깅 최적화
    LoggingConfig.configure_spark_logging()
    
    # 기본 로거 설정 (프로젝트용)
    project_logger = get_logger('social_media_analysis')
    project_logger.info("프로젝트 로깅 시스템이 초기화되었습니다.")
    
    return project_logger

# 전역 로거 인스턴스 (import 시 자동 생성)
if __name__ != "__main__":
    setup_project_logging()