"""
한국 시간대(KST) 유틸리티 모듈
프로젝트 전체에서 일관된 시간대 처리를 위한 헬퍼 함수들
"""

from datetime import datetime, timezone, timedelta
from typing import Optional

# 한국 시간대 정의 (UTC+9)
KST = timezone(timedelta(hours=9))

def now_kst() -> datetime:
    """
    현재 시간을 한국 시간대(KST)로 반환
    
    Returns:
        datetime: 한국 시간대 현재 시간
    """
    return datetime.now(KST)

def utc_to_kst(utc_time: datetime) -> datetime:
    """
    UTC 시간을 한국 시간대로 변환
    
    Args:
        utc_time: UTC 시간
        
    Returns:
        datetime: 한국 시간대로 변환된 시간
    """
    if utc_time.tzinfo is None:
        utc_time = utc_time.replace(tzinfo=timezone.utc)
    return utc_time.astimezone(KST)

def kst_to_utc(kst_time: datetime) -> datetime:
    """
    한국 시간을 UTC로 변환
    
    Args:
        kst_time: 한국 시간
        
    Returns:
        datetime: UTC로 변환된 시간
    """
    if kst_time.tzinfo is None:
        kst_time = kst_time.replace(tzinfo=KST)
    return kst_time.astimezone(timezone.utc)

def format_kst_time(dt: Optional[datetime] = None, format_str: str = "%Y-%m-%d %H:%M:%S KST") -> str:
    """
    한국 시간을 포맷된 문자열로 반환
    
    Args:
        dt: 변환할 datetime 객체 (None이면 현재 시간)
        format_str: 시간 포맷 문자열
        
    Returns:
        str: 포맷된 시간 문자열
    """
    if dt is None:
        dt = now_kst()
    elif dt.tzinfo is None:
        dt = dt.replace(tzinfo=KST)
    elif dt.tzinfo != KST:
        dt = dt.astimezone(KST)
    
    return dt.strftime(format_str)

def get_kst_timezone():
    """
    KST 시간대 객체 반환
    
    Returns:
        timezone: 한국 시간대 객체
    """
    return KST