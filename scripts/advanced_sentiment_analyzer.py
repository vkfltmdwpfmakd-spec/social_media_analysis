"""
고도화된 감성 분석 모듈
- 다중 API 지원 (OpenAI, Hugging Face, 로컬 모델)
- 폴백 메커니즘으로 안정성 보장
- 캐싱을 통한 성능 최적화
- 배치 처리 지원
"""

import json
import time
import hashlib
from typing import List, Dict, Tuple, Optional, Union
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging

import pandas as pd
# import aiohttp  # 임시로 주석 처리 - Spark 환경에서 의존성 문제 해결 후 활성화
import openai
from transformers import pipeline
import requests
from functools import lru_cache
import sys
import os

# 프로젝트 설정 import
try:
    from config import settings
    from config.logging_config import get_logger
except ImportError:
    # Spark 환경에서의 폴백 설정
    import sys
    import os
    
    # 기본 설정 클래스 정의
    class DefaultSettings:
        ENVIRONMENT = os.getenv('ENVIRONMENT', 'development')
        LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
        OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
        HUGGINGFACE_API_KEY = os.getenv('HUGGINGFACE_API_KEY', '')
        MAX_API_REQUESTS_PER_MINUTE = int(os.getenv('MAX_API_REQUESTS_PER_MINUTE', '50'))
        API_TIMEOUT_SECONDS = int(os.getenv('API_TIMEOUT_SECONDS', '10'))
    
    settings = DefaultSettings()
    
    def get_logger(name):
        import logging
        logging.basicConfig(level=getattr(logging, settings.LOG_LEVEL, logging.INFO))
        return logging.getLogger(name)

# --- Hugging Face 파이프라인 전역 변수 ---
# Spark UDF에서 사용 시, 각 Executor에서 한 번만 초기화되도록 전역으로 관리
hf_pipeline_ko = None
hf_pipeline_en = None

@dataclass
class SentimentResult:
    """감성 분석 결과 데이터 클래스"""
    label: str  # POSITIVE, NEGATIVE, NEUTRAL
    confidence: float  # 0.0 ~ 1.0
    source: str  # 'openai', 'huggingface', 'local'
    processing_time: float  # 처리 시간 (초)
    text_length: int  # 원본 텍스트 길이

class SentimentCache:
    """감성 분석 결과 캐싱 클래스"""
    
    def __init__(self, max_size: int = 10000, ttl_hours: int = 24):
        self.cache = {}
        self.max_size = max_size
        self.ttl = timedelta(hours=ttl_hours)
        self.logger = get_logger(__name__)
    
    def _get_cache_key(self, text: str, source: str) -> str:
        """캐시 키 생성"""
        content = f"{text}:{source}"
        return hashlib.md5(content.encode()).hexdigest()
    
    def get(self, text: str, source: str) -> Optional[SentimentResult]:
        """캐시에서 결과 조회"""
        key = self._get_cache_key(text, source)
        if key in self.cache:
            result, timestamp = self.cache[key]
            if datetime.now() - timestamp < self.ttl:
                return result
            else:
                # 만료된 캐시 삭제
                del self.cache[key]
        return None
    
    def set(self, text: str, source: str, result: SentimentResult):
        """캐시에 결과 저장"""
        if len(self.cache) >= self.max_size:
            # LRU 방식으로 오래된 항목 삭제
            oldest_key = min(self.cache.keys(), 
                            key=lambda k: self.cache[k][1])
            del self.cache[oldest_key]
        
        key = self._get_cache_key(text, source)
        self.cache[key] = (result, datetime.now())

class AdvancedSentimentAnalyzer:
    """고도화된 감성 분석기"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.cache = SentimentCache()
        self.api_request_count = {}  # API 요청 수 추적
        self.last_api_reset = datetime.now()
        
        # OpenAI 클라이언트 초기화
        if settings.OPENAI_API_KEY:
            openai.api_key = settings.OPENAI_API_KEY
            self.openai_available = True
        else:
            self.openai_available = False
            self.logger.warning("OpenAI API 키가 설정되지 않음")
        
        # Hugging Face 파이프라인 초기화 (지연 로딩)
        self.hf_pipeline_ko = None
        self.hf_pipeline_en = None
        self.hf_available = True  # 로컬 파이프라인 사용으로 변경
        
        self.logger.info("고도화된 감성 분석기 초기화 완료")
    
    def _reset_api_counter_if_needed(self):
        """API 요청 카운터 리셋 (분당 제한)"""
        now = datetime.now()
        if (now - self.last_api_reset).seconds >= 60:
            self.api_request_count = {}
            self.last_api_reset = now
    
    def _can_make_api_request(self, source: str) -> bool:
        """API 요청 제한 확인"""
        self._reset_api_counter_if_needed()
        current_count = self.api_request_count.get(source, 0)
        return current_count < settings.MAX_API_REQUESTS_PER_MINUTE
    
    def _increment_api_counter(self, source: str):
        """API 요청 카운터 증가"""
        self.api_request_count[source] = self.api_request_count.get(source, 0) + 1
    
    async def _analyze_with_openai(self, texts: List[str], language: str) -> List[SentimentResult]:
        """OpenAI API를 이용한 감성 분석"""
        if not self.openai_available or not self._can_make_api_request('openai'):
            return []
        
        results = []
        start_time = time.time()
        
        try:
            # 언어별 프롬프트
            if language == 'ko':
                system_prompt = """당신은 한국어 텍스트의 감정을 분석하는 전문가입니다. 
                주어진 텍스트의 감정을 POSITIVE, NEGATIVE, NEUTRAL 중 하나로 분류하세요.
                응답은 반드시 JSON 형식으로 해주세요: {"sentiment": "POSITIVE/NEGATIVE/NEUTRAL", "confidence": 0.95}"""
            else:
                system_prompt = """You are an expert at analyzing sentiment in English text.
                Classify the sentiment as POSITIVE, NEGATIVE, or NEUTRAL.
                Respond only in JSON format: {"sentiment": "POSITIVE/NEGATIVE/NEUTRAL", "confidence": 0.95}"""
            
            for text in texts:
                if len(text.strip()) == 0:
                    results.append(SentimentResult('NEUTRAL', 0.5, 'openai', 0.0, 0))
                    continue
                
                try:
                    response = await asyncio.get_event_loop().run_in_executor(
                        None,
                        lambda: openai.ChatCompletion.create(
                            model=settings.OPENAI_MODEL,
                            messages=[
                                {"role": "system", "content": system_prompt},
                                {"role": "user", "content": f"텍스트: {text[:500]}"}  # 500자 제한
                            ],
                            max_tokens=50,
                            timeout=settings.API_TIMEOUT_SECONDS
                        )
                    )
                    
                    content = response.choices[0].message.content.strip()
                    result_data = json.loads(content)
                    
                    sentiment = result_data.get('sentiment', 'NEUTRAL')
                    confidence = float(result_data.get('confidence', 0.5))
                    
                    processing_time = time.time() - start_time
                    result = SentimentResult(sentiment, confidence, 'openai', processing_time, len(text))
                    results.append(result)
                    
                    self._increment_api_counter('openai')
                    
                except (json.JSONDecodeError, KeyError, ValueError) as e:
                    self.logger.warning(f"OpenAI 응답 파싱 오류: {e}")
                    results.append(SentimentResult('NEUTRAL', 0.5, 'openai', 0.0, len(text)))
                
                except Exception as e:
                    self.logger.error(f"OpenAI API 호출 오류: {e}")
                    results.append(SentimentResult('NEUTRAL', 0.5, 'openai', 0.0, len(text)))
                    
        except Exception as e:
            self.logger.error(f"OpenAI 배치 처리 오류: {e}")
        
        return results
    
    def _analyze_with_local_keywords(self, texts: List[str], language: str) -> List[SentimentResult]:
        """로컬 키워드 기반 감성 분석 (폴백)"""
        results = []
        
        if language == 'ko':
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
        else:
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
        
        for text in texts:
            start_time = time.time()
            
            if not text or len(text.strip()) == 0:
                results.append(SentimentResult('NEUTRAL', 0.5, 'local', 0.0, 0))
                continue
            
            clean_text = text.lower()
            words = clean_text.split()
            
            pos_score = sum(1 for word in words if any(pw in word for pw in positive_words))
            neg_score = sum(1 for word in words if any(nw in word for nw in negative_words))
            
            if pos_score > neg_score and pos_score > 0:
                sentiment = 'POSITIVE'
                confidence = min(0.8, 0.5 + (pos_score * 0.1))
            elif neg_score > pos_score and neg_score > 0:
                sentiment = 'NEGATIVE' 
                confidence = min(0.8, 0.5 + (neg_score * 0.1))
            else:
                sentiment = 'NEUTRAL'
                confidence = 0.6
            
            processing_time = time.time() - start_time
            result = SentimentResult(sentiment, confidence, 'local', processing_time, len(text))
            results.append(result)
        
        return results
    
    def get_analytics(self) -> Dict:
        """분석 통계 반환"""
        return {
            'cache_size': len(self.cache.cache),
            'api_requests': self.api_request_count.copy(),
            'openai_available': self.openai_available,
            'huggingface_available': self.hf_available,
            'last_reset': self.last_api_reset.isoformat()
        }

# 전역 인스턴스 (싱글톤)
_sentiment_analyzer = None

def get_sentiment_analyzer() -> AdvancedSentimentAnalyzer:
    """전역 감성 분석기 인스턴스 반환"""
    global _sentiment_analyzer
    if _sentiment_analyzer is None:
        _sentiment_analyzer = AdvancedSentimentAnalyzer()
    return _sentiment_analyzer

def _initialize_hf_pipelines():
    """Hugging Face 파이프라인 초기화 함수"""
    global hf_pipeline_ko, hf_pipeline_en
    logger = get_logger(__name__)
    
    if hf_pipeline_ko is None:
        try:
            logger.info(f"Hugging Face 한국어 모델 로딩 시작: {settings.KO_SENTIMENT_MODEL}")
            hf_pipeline_ko = pipeline("sentiment-analysis", model=settings.KO_SENTIMENT_MODEL)
            logger.info("Hugging Face 한국어 모델 로딩 완료")
        except Exception as e:
            logger.error(f"Hugging Face 한국어 모델 로딩 실패: {e}")

    if hf_pipeline_en is None:
        try:
            logger.info(f"Hugging Face 영어 모델 로딩 시작: {settings.EN_SENTIMENT_MODEL}")
            hf_pipeline_en = pipeline("sentiment-analysis", model=settings.EN_SENTIMENT_MODEL)
            logger.info("Hugging Face 영어 모델 로딩 완료")
        except Exception as e:
            logger.error(f"Hugging Face 영어 모델 로딩 실패: {e}")

def _standardize_hf_label(label: str) -> str:
    """Hugging Face 모델의 라벨을 표준 형식으로 변환"""
    label_upper = label.upper()
    if 'POSITIVE' in label_upper or '4_STARS' in label_upper or '5_STARS' in label_upper:
        return 'POSITIVE'
    if 'NEGATIVE' in label_upper or '1_STAR' in label_upper or '2_STARS' in label_upper:
        return 'NEGATIVE'
    return 'NEUTRAL'

# PySpark UDF에서 사용할 동기 래퍼 함수 (Hugging Face 우선, 로컬 폴백)
def analyze_sentiment_batch_sync(texts: List[str], language: str = 'auto') -> List[str]:
    """PySpark UDF용 동기 배치 감성 분석 - Hugging Face 우선, 로컬 폴백"""
    analyzer = get_sentiment_analyzer()
    logger = get_logger(f"{__name__}.spark_udf")

    try:
        # 파이프라인 초기화 (필요 시)
        _initialize_hf_pipelines()

        # 언어 감지
        if language == 'auto':
            korean_chars = sum(1 for text in texts for char in text if '가' <= char <= '힣')
            total_chars = sum(len(text) for text in texts if text)
            language = 'ko' if total_chars > 0 and korean_chars / total_chars > 0.1 else 'en'
        
        selected_pipeline = hf_pipeline_ko if language == 'ko' else hf_pipeline_en

        if selected_pipeline:
            logger.info(f"Hugging Face 파이프라인으로 {len(texts)}개 텍스트 분석 (언어: {language})")
            # 텍스트가 비어있거나 None인 경우 처리
            valid_texts = [text if isinstance(text, str) else "" for text in texts]
            
            results = selected_pipeline(valid_texts, truncation=True, max_length=512)
            
            # 결과 표준화
            sentiment_labels = [_standardize_hf_label(res['label']) for res in results]
            
            label_counts = pd.Series(sentiment_labels).value_counts().to_dict()
            logger.info(f"Hugging Face 분석 완료. 감성 분포: {label_counts}")
            
            return sentiment_labels
        else:
            # Hugging Face 파이프라인 로딩 실패 시 로컬 키워드 분석으로 폴백
            logger.warning(f"Hugging Face 파이프라인 사용 불가. 로컬 키워드 분석으로 폴백합니다.")
            results = analyzer._analyze_with_local_keywords(texts, language)
            return [result.label for result in results]

    except Exception as e:
        logger.error(f"동기 배치 분석 중 심각한 오류 발생: {e}", exc_info=True)
        # 최종 폴백: 로컬 키워드 분석
        logger.warning("최종 폴백: 로컬 키워드 분석을 시도합니다.")
        try:
            results = analyzer._analyze_with_local_keywords(texts, language)
            return [result.label for result in results]
        except Exception as fallback_e:
            logger.error(f"로컬 키워드 분석마저 실패: {fallback_e}")
            return ["NEUTRAL"] * len(texts)
