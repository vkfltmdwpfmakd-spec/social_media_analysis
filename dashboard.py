import streamlit as st
import pandas as pd
import requests
import json
import time
import os
import sys
from urllib.parse import urlparse
try:
    from hdfs import InsecureClient
    HDFS_AVAILABLE = True
except ImportError:
    HDFS_AVAILABLE = False
    
# Spark 의존성 완전 제거
SPARK_AVAILABLE = False

# 프로젝트 루트 경로를 sys.path에 추가
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
try:
    from config import settings
except ImportError:
    # 설정 파일이 없을 때 기본값 사용
    class Settings:
        SENTIMENT_DATA_PATH = "hdfs://namenode:8020/user/spark/social_sentiment_data"
    settings = Settings()

# --- 페이지 설정 ---
st.set_page_config(
    page_title="실시간 소셜 미디어 감성 분석 대시보드",
    page_icon="📊",
    layout="wide",
)

# --- HDFS 직접 연결 사용 (Spark 없이) ---
@st.cache_resource
def get_hdfs_client():
    """HDFS 클라이언트를 생성하고 반환합니다."""
    if not HDFS_AVAILABLE:
        st.warning("⚠️ HDFS 라이브러리가 설치되지 않았습니다. WebHDFS API만 사용합니다.")
        return None
    try:
        # Docker 네트워크 내부 URL 대신 localhost 사용
        hdfs_url = "http://localhost:9870"  # WebHDFS 접근
        client = InsecureClient(hdfs_url, user="spark")
        return client
    except Exception as e:
        st.error(f"HDFS 클라이언트 연결 실패: {e}")
        return None

# --- 대안: WebHDFS API 직접 호출 ---
def list_hdfs_files(path):
    """WebHDFS API를 사용하여 HDFS 파일 목록을 가져옵니다."""
    webhdfs_urls = [
        "http://localhost:9870/webhdfs/v1",
        "http://127.0.0.1:9870/webhdfs/v1",
        "http://namenode:9870/webhdfs/v1"
    ]
    
    st.write(f"🌐 HDFS 연결 시도 중... (경로: {path})")
    
    for i, webhdfs_url in enumerate(webhdfs_urls):
        try:
            st.write(f"🔗 시도 {i+1}: {webhdfs_url}")
            response = requests.get(f"{webhdfs_url}{path}", params={"op": "LISTSTATUS"}, timeout=5)
            st.write(f"📡 응답 상태: {response.status_code}")
            if response.status_code == 200:
                files = response.json().get("FileStatuses", {}).get("FileStatus", [])
                st.write(f"✅ 성공! {len(files)}개 파일 발견")
                return files
            else:
                st.write(f"❌ 실패: HTTP {response.status_code}")
        except Exception as e:
            st.write(f"❌ 연결 오류: {str(e)[:50]}")
            continue
    
    st.error(f"WebHDFS API 호출 실패: 모든 URL 연결 시도 실패")
    return []

def read_hdfs_file_content(file_path):
    """WebHDFS API를 사용하여 HDFS 파일 내용을 읽어옵니다."""
    try:
        webhdfs_url = "http://localhost:9870/webhdfs/v1"
        response = requests.get(f"{webhdfs_url}{file_path}", params={"op": "OPEN"}, timeout=30)
        if response.status_code == 200:
            return response.content
        return None
    except Exception as e:
        st.error(f"HDFS 파일 읽기 실패: {e}")
        return None

def load_data_from_hdfs():
    """WebHDFS API를 사용하여 HDFS에서 데이터를 로드합니다."""
    # 디버깅용 출력
    st.write("🔄 load_data_from_hdfs() 함수 실행됨")
    try:
        hdfs_path = "/user/spark/social_sentiment_data"  # HDFS 경로
        
        # HDFS 디렉토리의 파일 목록 가져오기
        files = list_hdfs_files(hdfs_path)
        st.write(f"🔍 list_hdfs_files() 결과: {len(files) if files else 0}개 파일")
        
        if not files:
            st.warning("⚠️ HDFS에서 파일을 찾을 수 없습니다.")
            st.info("아직 분석된 데이터가 없습니다. 데이터 수집 및 처리 중입니다...")
            return pd.DataFrame()
        
        # parquet 파일만 필터링
        parquet_files = [f for f in files if f.get('pathSuffix', '').endswith('.parquet') and f.get('length', 0) > 0]
        st.write(f"🎯 Parquet 파일 필터링 결과: {len(parquet_files)}개 파일")
        
        if not parquet_files:
            st.warning(f"⚠️ {len(files)}개 파일이 있지만 유효한 Parquet 파일이 없습니다.")
            st.info("아직 분석된 데이터가 없습니다. 파이프라인이 실행되고 데이터가 수집될 때까지 기다려주세요.")
            return pd.DataFrame()
        
        # 실제 Parquet 파일에서 데이터 읽기 시도
        st.success(f"📁 {len(parquet_files)}개의 Parquet 파일을 HDFS에서 발견했습니다!")
        
        # 파일 정보 요약 표시
        total_size = sum(f.get('length', 0) for f in parquet_files)
        st.write(f"📊 총 데이터 크기: {total_size:,} bytes")
        
        # 전체 파일을 읽되, 크기가 큰 파일들 우선 (최대 10개)
        # 파일 크기 순으로 정렬하여 안정적인 데이터 확보
        latest_files = sorted(parquet_files, key=lambda x: x.get('length', 0), reverse=True)[:10]
        st.info(f"📊 총 {len(parquet_files)}개 파일 중 상위 {len(latest_files)}개 파일에서 데이터 로드")
        
        all_data = []
        successful_reads = 0
        
        for file_info in latest_files:
            file_name = file_info.get('pathSuffix', '')
            file_path = f"{hdfs_path}/{file_name}"
            
            try:
                # WebHDFS API로 파일 내용 읽기 (namenode 사용)
                webhdfs_urls = [
                    "http://namenode:9870/webhdfs/v1",
                    "http://localhost:9870/webhdfs/v1", 
                    "http://127.0.0.1:9870/webhdfs/v1"
                ]
                
                response = None
                for webhdfs_url in webhdfs_urls:
                    try:
                        response = requests.get(f"{webhdfs_url}{file_path}", params={"op": "OPEN"}, timeout=30)
                        if response.status_code == 200:
                            break
                    except requests.exceptions.RequestException:
                        continue
                
                if response and response.status_code == 200:
                    # parquet 파일을 메모리에서 읽기
                    import io
                    parquet_buffer = io.BytesIO(response.content)
                    
                    try:
                        df_chunk = pd.read_parquet(parquet_buffer)
                        all_data.append(df_chunk)
                        successful_reads += 1
                        st.write(f"✅ 읽기 성공: {file_name} ({len(df_chunk)} 레코드)")
                    except Exception as e:
                        st.write(f"⚠️ Parquet 파싱 실패: {file_name} - {str(e)[:50]}")
                else:
                    status_code = response.status_code if response else "연결 실패"
                    st.write(f"❌ 파일 읽기 실패: {file_name} - HTTP {status_code}")
                    
            except Exception as e:
                st.write(f"❌ 연결 오류: {file_name} - {str(e)[:50]}")
        
        if all_data and successful_reads > 0:
            # 모든 데이터를 하나로 합치기
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 시간대 정보 처리 - Spark에서 이미 한국시간으로 변환되어 저장됨
            if 'created_at' in combined_df.columns:
                try:
                    # created_at이 문자열인 경우 datetime으로 변환
                    if combined_df['created_at'].dtype == 'object':
                        combined_df['created_at'] = pd.to_datetime(combined_df['created_at'])
                    
                    # Spark에서 이미 Asia/Seoul로 변환했으므로 시간대 정보만 추가
                    if combined_df['created_at'].dt.tz is None:
                        combined_df['created_at'] = combined_df['created_at'].dt.tz_localize('Asia/Seoul')
                    
                    st.info(f"⏰ 시간 처리 완료. 샘플 시간: {combined_df['created_at'].iloc[0]}")
                    
                except Exception as e:
                    st.warning(f"시간대 변환 실패: {e}")
            
            # 최신 순으로 정렬
            if 'created_at' in combined_df.columns:
                combined_df = combined_df.sort_values(by="created_at", ascending=False)
            
            st.success(f"🎉 총 {len(combined_df)}개의 실제 데이터를 로드했습니다!")
            return combined_df
            
        else:
            # 데이터 읽기 실패 시 경고 메시지만 표시
            st.warning("⚠️ 실제 데이터를 읽을 수 없어 빈 데이터를 반환합니다.")
            st.info("데이터 파이프라인이 실행 중이거나 parquet 파일 형식에 문제가 있을 수 있습니다.")
            return pd.DataFrame()
        
    except Exception as e:
        st.error(f"HDFS 데이터 로딩 중 오류 발생: {e}")
        return pd.DataFrame()


# --- 대시보드 UI ---
st.title("📊 실시간 소셜 미디어 감성 분석 대시보드")

# 캐시 완전 초기화 (매번 새로운 데이터 로드)
st.cache_data.clear()
st.cache_resource.clear()

# 강제 리프레시를 위한 타임스탬프 추가
current_time = int(time.time())
st.write(f"🕐 로드 시간: {current_time}")

df = load_data_from_hdfs()

if df.empty:
    st.warning("아직 분석된 데이터가 없습니다. 파이프라인이 실행되고 데이터가 수집될 때까지 기다려주세요.")
    
    # 데이터 수집 상태 체크
    with st.expander("파이프라인 상태 체크"):
        try:
            # WebHDFS API를 사용한 상태 체크 (여러 URL 시도)
            webhdfs_urls = [
                "http://localhost:9870/webhdfs/v1",
                "http://127.0.0.1:9870/webhdfs/v1",
                "http://namenode:9870/webhdfs/v1"  # Docker 네트워크 내부에서 실행 시
            ]
            
            # HDFS 연결 체크 (여러 URL 시도)
            connected = False
            working_url = None
            
            for webhdfs_url in webhdfs_urls:
                try:
                    response = requests.get(f"{webhdfs_url}/", params={"op": "LISTSTATUS"}, timeout=3)
                    if response.status_code == 200:
                        st.success(f"✅ HDFS WebHDFS 연결 성공: {webhdfs_url}")
                        working_url = webhdfs_url
                        connected = True
                        break
                except requests.exceptions.RequestException:
                    continue
            
            if connected:
                # 데이터 디렉토리 체크
                data_files = list_hdfs_files("/user/spark/social_sentiment_data")
                if data_files:
                    parquet_count = len([f for f in data_files if f.get('pathSuffix', '').endswith('.parquet')])
                    st.info(f"📁 데이터 디렉토리 존재: {len(data_files)}개 파일 ({parquet_count}개 Parquet)")
                else:
                    st.warning("⚠️ 데이터 디렉토리가 비어있거나 아직 생성되지 않음")
            else:
                st.error("❌ 모든 HDFS URL에 연결 실패")
                st.info("🔧 시도한 URL: " + ", ".join(webhdfs_urls))
                
            # Docker 서비스 상태 체크 안내
            st.info("🐋 Docker 서비스 상태 확인: `docker ps` 명령으로 확인해주세요.")
            st.info("🔌 필요한 서비스: namenode, datanode, spark-streaming-analyzer")
                
        except Exception as e:
            st.error(f"❌ 상태 체크 실패: {str(e)[:100]}...")
else:
    total_tweets = len(df)
    
    # --- 데이터 전처리 ---
    # 시간 형식을 읽기 쉽게 변환 (연도 포함)
    if 'created_at' in df.columns:
        df['formatted_time'] = df['created_at'].dt.strftime('%Y-%m-%d %H:%M')
    
    # 실제 sentiment_label 값들 확인
    if 'sentiment_label' in df.columns:
        unique_sentiments = df['sentiment_label'].unique()
        st.info(f"🔍 실제 감성 라벨 값들: {list(unique_sentiments)}")
        sentiment_counts_raw = df['sentiment_label'].value_counts()
        st.write("**각 라벨 개수:**")
        for label, count in sentiment_counts_raw.items():
            st.write(f"• {label}: {count}개")
    
    # 유효한 감성 라벨만 필터링 (실제 값에 맞춰 조정)
    valid_sentiments = ['POSITIVE', 'NEGATIVE', 'NEUTRAL', 'positive', 'negative', 'neutral']
    df_valid = df[df['sentiment_label'].isin(valid_sentiments)].copy()
    
    # 만약 여전히 비어있다면 모든 데이터 표시 (디버깅용)
    if df_valid.empty and not df.empty:
        st.warning("⚠️ 유효한 감성 라벨이 없어서 모든 데이터를 표시합니다.")
        df_valid = df.copy()
    
    # 언어별로 데이터 분리
    if 'detected_language' in df.columns:
        df_korean = df_valid[df_valid['detected_language'] == 'ko'].copy()
        df_english = df_valid[df_valid['detected_language'] == 'en'].copy()
        df_other = df_valid[~df_valid['detected_language'].isin(['ko', 'en'])].copy()
    else:
        df_korean = df_valid.copy()
        df_english = pd.DataFrame()
        df_other = pd.DataFrame()
    
    # --- 상단 KPI ---
    col_kpi1, col_kpi2, col_kpi3 = st.columns(3)
    with col_kpi1:
        st.metric(label="총 수집 항목", value=f"{total_tweets:,} 개")
    with col_kpi2:
        st.metric(label="유효 분석 데이터", value=f"{len(df_valid):,} 개")
    with col_kpi3:
        error_count = total_tweets - len(df_valid)
        st.metric(label="분석 오류", value=f"{error_count:,} 개")
    
    # --- 감성 분석 결과 ---
    if not df_valid.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("전체 감성 분포")
            sentiment_counts = df_valid['sentiment_label'].value_counts()
            st.bar_chart(sentiment_counts)

        with col2:
            st.subheader("시간대별 항목량")
            if 'created_at' in df_valid.columns:
                df_valid['hour'] = df_valid['created_at'].dt.hour
                hourly_counts = df_valid.groupby('hour').size()
                st.line_chart(hourly_counts)

    # --- 언어별 데이터 표시 ---
    tab1, tab2, tab3 = st.tabs(["🇰🇷 한국어", "🇺🇸 영어", "🌍 기타"])
    
    with tab1:
        if not df_korean.empty:
            st.subheader(f"한국어 데이터 ({len(df_korean)}개)")
            
            # 한국어 감성 분포
            if len(df_korean) > 0:
                korean_sentiment = df_korean['sentiment_label'].value_counts()
                col_k1, col_k2 = st.columns(2)
                with col_k1:
                    st.write("**감성 분포**")
                    st.bar_chart(korean_sentiment)
                with col_k2:
                    st.write("**감성 비율**")
                    for sentiment, count in korean_sentiment.items():
                        percentage = (count / len(df_korean)) * 100
                        st.write(f"• {sentiment}: {count}개 ({percentage:.1f}%)")
            
            # 최신 한국어 데이터
            st.write("**최신 한국어 항목**")
            display_columns = ['formatted_time', 'text', 'sentiment_label']
            available_columns = [col for col in display_columns if col in df_korean.columns]
            if available_columns:
                st.dataframe(
                    df_korean[available_columns].head(10),
                    column_config={
                        'formatted_time': '시간',
                        'text': '내용', 
                        'sentiment_label': '감성'
                    }
                )
        else:
            st.info("한국어 데이터가 없습니다.")
    
    with tab2:
        if not df_english.empty:
            st.subheader(f"영어 데이터 ({len(df_english)}개)")
            
            # 영어 감성 분포
            if len(df_english) > 0:
                english_sentiment = df_english['sentiment_label'].value_counts()
                col_e1, col_e2 = st.columns(2)
                with col_e1:
                    st.write("**Sentiment Distribution**")
                    st.bar_chart(english_sentiment)
                with col_e2:
                    st.write("**Sentiment Ratio**")
                    for sentiment, count in english_sentiment.items():
                        percentage = (count / len(df_english)) * 100
                        st.write(f"• {sentiment}: {count} items ({percentage:.1f}%)")
            
            # 최신 영어 데이터  
            st.write("**Latest English Items**")
            display_columns = ['formatted_time', 'text', 'sentiment_label']
            available_columns = [col for col in display_columns if col in df_english.columns]
            if available_columns:
                st.dataframe(
                    df_english[available_columns].head(10),
                    column_config={
                        'formatted_time': 'Time',
                        'text': 'Content', 
                        'sentiment_label': 'Sentiment'
                    }
                )
        else:
            st.info("No English data available.")
            
    with tab3:
        if not df_other.empty:
            st.subheader(f"기타 언어 데이터 ({len(df_other)}개)")
            display_columns = ['formatted_time', 'text', 'sentiment_label', 'detected_language']
            available_columns = [col for col in display_columns if col in df_other.columns]
            if available_columns:
                st.dataframe(df_other[available_columns].head(10))
        else:
            st.info("기타 언어 데이터가 없습니다.")

# 자동 새로고침 섹션을 조건부로 처리
if not df.empty:
    # 마지막 업데이트 시간 표시
    last_update = df['created_at'].max() if 'created_at' in df.columns else "알 수 없음"
    st.info(f"📊 마지막 데이터 업데이트: {last_update}")
    st.info("대시보드는 30초마다 자동으로 새로고침됩니다.")

# 자동 새로고침 (조건부) - 더 빠른 새로고침
auto_refresh = st.checkbox("자동 새로고침 활성화", value=True)  # 기본값 true로 변경
if auto_refresh:
    placeholder = st.empty()
    time.sleep(30)  # 30초로 단축
    st.rerun()