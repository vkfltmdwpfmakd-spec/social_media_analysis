"""
에러 처리 및 복구 관리 모듈
- 시스템 레벨 에러 모니터링
- 자동 복구 메커니즘
- 장애 알림 시스템
- 헬스 체크 및 자가 치유
"""

import sys
import os
import time
import traceback
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Callable, Any
from dataclasses import dataclass
from enum import Enum
import asyncio
import aiohttp
from contextlib import asynccontextmanager

# 프로젝트 모듈 import
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import settings
from config.logging_config import get_logger

class ErrorSeverity(Enum):
    """에러 심각도 레벨"""
    LOW = "LOW"          # 경고 수준, 서비스 지속 가능
    MEDIUM = "MEDIUM"    # 일부 기능 영향, 모니터링 필요
    HIGH = "HIGH"        # 주요 기능 장애, 즉시 대응 필요
    CRITICAL = "CRITICAL" # 전체 서비스 중단, 긴급 대응

class RecoveryStrategy(Enum):
    """복구 전략"""
    RETRY = "RETRY"              # 재시도
    FALLBACK = "FALLBACK"        # 폴백 메커니즘
    RESTART = "RESTART"          # 서비스 재시작
    MANUAL = "MANUAL"            # 수동 개입 필요

@dataclass
class ErrorEvent:
    """에러 이벤트 데이터 클래스"""
    timestamp: datetime
    component: str
    error_type: str
    message: str
    severity: ErrorSeverity
    stack_trace: Optional[str] = None
    context: Optional[Dict[str, Any]] = None
    recovery_attempted: bool = False
    recovery_successful: bool = False

class CircuitBreaker:
    """서킷 브레이커 패턴 구현"""
    
    def __init__(self, failure_threshold: int = 5, timeout: int = 60):
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.failure_count = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.logger = get_logger(__name__)
    
    def call(self, func: Callable, *args, **kwargs):
        """서킷 브레이커를 통한 함수 호출"""
        if self.state == "OPEN":
            if self.last_failure_time and \
               (datetime.now() - self.last_failure_time).seconds > self.timeout:
                self.state = "HALF_OPEN"
                self.logger.info("서킷 브레이커: HALF_OPEN 상태로 전환")
            else:
                raise Exception("서킷 브레이커가 OPEN 상태입니다")
        
        try:
            result = func(*args, **kwargs)
            if self.state == "HALF_OPEN":
                self.state = "CLOSED"
                self.failure_count = 0
                self.logger.info("서킷 브레이커: CLOSED 상태로 복구")
            return result
            
        except Exception as e:
            self.failure_count += 1
            self.last_failure_time = datetime.now()
            
            if self.failure_count >= self.failure_threshold:
                self.state = "OPEN"
                self.logger.warning(f"서킷 브레이커: OPEN 상태로 전환 (실패 {self.failure_count}회)")
            
            raise e

class ErrorRecoveryManager:
    """에러 처리 및 복구 관리자"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.error_history: List[ErrorEvent] = []
        self.circuit_breakers: Dict[str, CircuitBreaker] = {}
        self.recovery_strategies: Dict[str, RecoveryStrategy] = {}
        self.health_checkers: Dict[str, Callable] = {}
        
        # 기본 복구 전략 설정
        self._setup_default_strategies()
        
        self.logger.info("에러 복구 관리자 초기화 완료")
    
    def _setup_default_strategies(self):
        """기본 복구 전략 설정"""
        self.recovery_strategies.update({
            # 네트워크 관련 에러
            'ConnectionError': RecoveryStrategy.RETRY,
            'TimeoutError': RecoveryStrategy.RETRY,
            'HTTPError': RecoveryStrategy.FALLBACK,
            
            # API 관련 에러
            'OpenAIError': RecoveryStrategy.FALLBACK,
            'HuggingFaceError': RecoveryStrategy.FALLBACK,
            
            # 데이터 처리 에러
            'DataProcessingError': RecoveryStrategy.FALLBACK,
            'ValidationError': RecoveryStrategy.RETRY,
            
            # 시스템 에러
            'OutOfMemoryError': RecoveryStrategy.RESTART,
            'DiskSpaceError': RecoveryStrategy.MANUAL,
            
            # Spark 관련 에러
            'SparkError': RecoveryStrategy.RESTART,
            'HDFSError': RecoveryStrategy.RETRY,
        })
    
    def register_circuit_breaker(self, component: str, 
                                failure_threshold: int = 5, 
                                timeout: int = 60):
        """컴포넌트별 서킷 브레이커 등록"""
        self.circuit_breakers[component] = CircuitBreaker(failure_threshold, timeout)
        self.logger.info(f"서킷 브레이커 등록: {component}")
    
    def register_health_checker(self, component: str, checker_func: Callable):
        """헬스 체크 함수 등록"""
        self.health_checkers[component] = checker_func
        self.logger.info(f"헬스 체커 등록: {component}")
    
    def log_error(self, component: str, error: Exception, 
                  severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                  context: Optional[Dict[str, Any]] = None) -> ErrorEvent:
        """에러 로깅 및 이벤트 생성"""
        error_event = ErrorEvent(
            timestamp=datetime.now(),
            component=component,
            error_type=type(error).__name__,
            message=str(error),
            severity=severity,
            stack_trace=traceback.format_exc(),
            context=context or {}
        )
        
        self.error_history.append(error_event)
        
        # 로그 레벨 결정
        if severity == ErrorSeverity.CRITICAL:
            self.logger.critical(
                f"CRITICAL ERROR in {component}: {error_event.message}",
                extra={'component': component, 'context': context}
            )
        elif severity == ErrorSeverity.HIGH:
            self.logger.error(
                f"HIGH SEVERITY ERROR in {component}: {error_event.message}",
                extra={'component': component, 'context': context}
            )
        elif severity == ErrorSeverity.MEDIUM:
            self.logger.warning(
                f"MEDIUM ERROR in {component}: {error_event.message}",
                extra={'component': component, 'context': context}
            )
        else:
            self.logger.info(
                f"LOW PRIORITY ERROR in {component}: {error_event.message}",
                extra={'component': component, 'context': context}
            )
        
        return error_event
    
    async def attempt_recovery(self, error_event: ErrorEvent) -> bool:
        """자동 복구 시도"""
        component = error_event.component
        error_type = error_event.error_type
        
        # 복구 전략 결정
        strategy = self.recovery_strategies.get(error_type, RecoveryStrategy.MANUAL)
        
        self.logger.info(f"복구 시도: {component} - {strategy.value}")
        
        try:
            if strategy == RecoveryStrategy.RETRY:
                return await self._retry_operation(error_event)
            elif strategy == RecoveryStrategy.FALLBACK:
                return await self._execute_fallback(error_event)
            elif strategy == RecoveryStrategy.RESTART:
                return await self._restart_component(error_event)
            else:
                self.logger.warning(f"수동 복구 필요: {component}")
                return False
                
        except Exception as recovery_error:
            self.logger.error(f"복구 시도 중 오류: {recovery_error}")
            return False
    
    async def _retry_operation(self, error_event: ErrorEvent) -> bool:
        """재시도 복구 전략"""
        component = error_event.component
        max_retries = 3
        base_delay = 1
        
        for attempt in range(max_retries):
            try:
                delay = base_delay * (2 ** attempt)  # 지수 백오프
                await asyncio.sleep(delay)
                
                self.logger.info(f"재시도 {attempt + 1}/{max_retries}: {component}")
                
                # 헬스 체크 실행
                if component in self.health_checkers:
                    health_check = self.health_checkers[component]
                    if await self._run_health_check(health_check):
                        self.logger.info(f"재시도 성공: {component}")
                        return True
                
            except Exception as e:
                self.logger.warning(f"재시도 {attempt + 1} 실패: {e}")
        
        self.logger.error(f"모든 재시도 실패: {component}")
        return False
    
    async def _execute_fallback(self, error_event: ErrorEvent) -> bool:
        """폴백 복구 전략"""
        component = error_event.component
        
        # 컴포넌트별 폴백 로직
        if 'sentiment' in component.lower():
            self.logger.info("감성 분석 폴백: 키워드 기반 분석으로 전환")
            return True
        elif 'api' in component.lower():
            self.logger.info("API 폴백: 로컬 처리로 전환")
            return True
        elif 'database' in component.lower():
            self.logger.info("데이터베이스 폴백: 캐시 데이터 사용")
            return True
        
        return False
    
    async def _restart_component(self, error_event: ErrorEvent) -> bool:
        """컴포넌트 재시작 전략"""
        component = error_event.component
        
        self.logger.warning(f"컴포넌트 재시작 시도: {component}")
        
        # 실제 환경에서는 Docker API나 Kubernetes API 사용
        # 여기서는 시뮬레이션
        try:
            await asyncio.sleep(5)  # 재시작 시뮬레이션
            self.logger.info(f"컴포넌트 재시작 완료: {component}")
            return True
        except Exception as e:
            self.logger.error(f"컴포넌트 재시작 실패: {e}")
            return False
    
    async def _run_health_check(self, health_check_func: Callable) -> bool:
        """헬스 체크 실행"""
        try:
            if asyncio.iscoroutinefunction(health_check_func):
                result = await health_check_func()
            else:
                result = health_check_func()
            return bool(result)
        except Exception as e:
            self.logger.error(f"헬스 체크 실행 오류: {e}")
            return False
    
    async def periodic_health_check(self, interval: int = 60):
        """주기적 헬스 체크"""
        while True:
            try:
                self.logger.debug("주기적 헬스 체크 시작")
                
                for component, health_checker in self.health_checkers.items():
                    try:
                        is_healthy = await self._run_health_check(health_checker)
                        if not is_healthy:
                            error_event = ErrorEvent(
                                timestamp=datetime.now(),
                                component=component,
                                error_type="HealthCheckFailed",
                                message=f"{component} 헬스 체크 실패",
                                severity=ErrorSeverity.HIGH
                            )
                            
                            self.error_history.append(error_event)
                            await self.attempt_recovery(error_event)
                            
                    except Exception as e:
                        self.logger.error(f"헬스 체크 오류 ({component}): {e}")
                
                await asyncio.sleep(interval)
                
            except Exception as e:
                self.logger.error(f"주기적 헬스 체크 중 오류: {e}")
                await asyncio.sleep(interval)
    
    def get_error_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """에러 통계 조회"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        recent_errors = [e for e in self.error_history if e.timestamp > cutoff_time]
        
        # 컴포넌트별 에러 수
        component_errors = {}
        for error in recent_errors:
            component_errors[error.component] = component_errors.get(error.component, 0) + 1
        
        # 심각도별 에러 수
        severity_errors = {}
        for error in recent_errors:
            severity_errors[error.severity.value] = severity_errors.get(error.severity.value, 0) + 1
        
        # 복구 성공률
        recovery_attempts = [e for e in recent_errors if e.recovery_attempted]
        recovery_success_rate = 0
        if recovery_attempts:
            successful_recoveries = [e for e in recovery_attempts if e.recovery_successful]
            recovery_success_rate = len(successful_recoveries) / len(recovery_attempts) * 100
        
        return {
            'total_errors': len(recent_errors),
            'component_breakdown': component_errors,
            'severity_breakdown': severity_errors,
            'recovery_success_rate': recovery_success_rate,
            'circuit_breaker_states': {
                comp: cb.state for comp, cb in self.circuit_breakers.items()
            }
        }
    
    async def send_alert(self, error_event: ErrorEvent):
        """알림 전송 (이메일, Slack 등)"""
        if error_event.severity in [ErrorSeverity.HIGH, ErrorSeverity.CRITICAL]:
            # 실제 환경에서는 이메일/Slack API 사용
            self.logger.critical(
                f"🚨 ALERT: {error_event.severity.value} error in {error_event.component}"
            )
            
            # 예시: Slack webhook (실제 구현 시 사용)
            # await self._send_slack_alert(error_event)
    
    @asynccontextmanager
    async def error_handler(self, component: str, 
                           severity: ErrorSeverity = ErrorSeverity.MEDIUM,
                           auto_recover: bool = True):
        """컨텍스트 매니저를 통한 에러 처리"""
        try:
            yield
        except Exception as e:
            error_event = self.log_error(component, e, severity)
            
            # 알림 전송
            await self.send_alert(error_event)
            
            # 자동 복구 시도
            if auto_recover:
                error_event.recovery_attempted = True
                recovery_successful = await self.attempt_recovery(error_event)
                error_event.recovery_successful = recovery_successful
                
                if not recovery_successful:
                    self.logger.error(f"자동 복구 실패: {component}")
            
            # 에러 재발생 (호출자가 처리할 수 있도록)
            raise

# 전역 인스턴스
_error_manager = None

def get_error_manager() -> ErrorRecoveryManager:
    """전역 에러 관리자 인스턴스 반환"""
    global _error_manager
    if _error_manager is None:
        _error_manager = ErrorRecoveryManager()
    return _error_manager

# 데코레이터 함수들
def with_circuit_breaker(component: str):
    """서킷 브레이커 데코레이터"""
    def decorator(func):
        def wrapper(*args, **kwargs):
            manager = get_error_manager()
            if component not in manager.circuit_breakers:
                manager.register_circuit_breaker(component)
            
            cb = manager.circuit_breakers[component]
            return cb.call(func, *args, **kwargs)
        return wrapper
    return decorator

def with_error_recovery(component: str, severity: ErrorSeverity = ErrorSeverity.MEDIUM):
    """에러 복구 데코레이터"""
    def decorator(func):
        async def async_wrapper(*args, **kwargs):
            manager = get_error_manager()
            async with manager.error_handler(component, severity):
                return await func(*args, **kwargs)
        
        def sync_wrapper(*args, **kwargs):
            manager = get_error_manager()
            # 동기 함수의 경우 기본적인 에러 로깅만 수행
            try:
                return func(*args, **kwargs)
            except Exception as e:
                manager.log_error(component, e, severity)
                raise
        
        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper
    return decorator