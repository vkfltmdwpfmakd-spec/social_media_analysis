import streamlit as st
import pandas as pd
import json
import logging
import os
import io
from hdfs import InsecureClient
from datetime import timedelta

# --- 환경 변수 및 기본 설정 ---
# Docker 컨테이너 환경에서 HDFS와 연결하기 위한 설정들
HDFS_NAMENODE_HOST = os.getenv('HDFS_NAMENODE_HOST', 'namenode')
HDFS_WEB_PORT = int(os.getenv('HDFS_WEB_PORT', 9870))
DASHBOARD_REFRESH_SECONDS = int(os.getenv('DASHBOARD_REFRESH_SECONDS', 30))

# HDFS에 저장된 데이터 경로들 - Spark가 parquet 파일로 저장하는 위치
HDFS_DATA_PATH = "/user/spark/social_sentiment_data"  # 실시간 스트리밍 데이터
REPORT_PATH = "/user/spark/daily_sentiment_reports"   # 일별 집계 리포트

# HDFS 클라이언트 초기화 - WebHDFS API 사용
client = InsecureClient(f'http://{HDFS_NAMENODE_HOST}:{HDFS_WEB_PORT}', user='root')

# 로깅 설정 - 대시보드 실행 상태 모니터링용
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 데이터 로딩 함수 ---
def load_hdfs_data(hdfs_path):
    """
    HDFS에서 모든 Parquet 파일을 읽어 Pandas DataFrame으로 반환
    
    Spark Streaming이 생성한 파티션별 parquet 파일들을 
    모두 읽어서 하나의 DataFrame으로 합치는 함수
    """
    try:
        # HDFS 경로가 존재하는지 확인
        if not client.status(hdfs_path, strict=False):
            logger.warning(f"HDFS 경로를 찾을 수 없습니다: {hdfs_path}")
            return pd.DataFrame()

        # 디렉토리 내 모든 파일 목록 가져오기
        file_list = client.list(hdfs_path)
        # .parquet 확장자를 가진 파일들만 필터링
        parquet_files = [f for f in file_list if f.endswith('.parquet')]
        
        if not parquet_files:
            return pd.DataFrame()

        # 각 parquet 파일을 읽어서 DataFrame 리스트에 저장
        all_dfs = []
        for file_name in parquet_files:
            full_path = os.path.join(hdfs_path, file_name)
            # HDFS에서 파일 읽기
            with client.read(full_path) as reader:
                # 메모리 버퍼에 저장 후 pandas로 읽기
                buffer = io.BytesIO(reader.read())
                df = pd.read_parquet(buffer)
                all_dfs.append(df)
        
        if not all_dfs:
            return pd.DataFrame()
            
        # 모든 DataFrame을 하나로 합치기
        return pd.concat(all_dfs, ignore_index=True)

    except Exception as e:
        logger.error(f"HDFS 데이터 로딩 중 오류 발생: {e}", exc_info=True)
        st.error(f"HDFS에서 데이터를 로드하는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

# --- Streamlit UI 구성 ---
st.set_page_config(layout="wide")
st.title("소셜 미디어 감성 분석 대시보드")

# --- 데이터 로딩 ---
# 배치 처리된 일일 리포트와 실시간 스트리밍 데이터를 각각 로드
report_df = load_hdfs_data(REPORT_PATH)        # Airflow DAG로 생성된 일별 집계 데이터
latest_data_df = load_hdfs_data(HDFS_DATA_PATH)  # Spark Streaming으로 실시간 수집된 데이터

# --- 일일 리포트 섹션 ---
st.header("📊 일일 감성 분석 리포트")
if not report_df.empty:
    # UTC 시간을 datetime으로 변환하고 최신 순으로 정렬
    report_df['report_date'] = pd.to_datetime(report_df['report_date'])
    report_df = report_df.sort_values(by='report_date', ascending=False)
    
    if not report_df.empty:
        # 가장 최신 리포트 데이터 선택
        latest_report = report_df.iloc[0]
        
        # UTC → KST 시간대 변환 (한국 시간 표시용)
        kst_time = latest_report['report_date'] + timedelta(hours=9)

        # 메트릭 카드로 주요 지표 표시
        col1, col2 = st.columns(2)
        with col1:
            st.metric("리포트 생성 시간 (KST)", kst_time.strftime('%Y-%m-%d %H:%M:%S'))
        with col2:
            st.metric("평균 감성 점수", f"{latest_report['average_sentiment_score']:.4f}")
        
        # 감성 점수 계산 방식 설명
        with st.expander("ℹ️ 평균 감성 점수 산정 기준 보기"):
            st.markdown("""
            - **Positive:** +1점
            - **Negative:** -1점  
            - **Neutral (또는 기타):** 0점
            
            위 기준으로 지난 24시간 동안 수집된 모든 데이터의 감성 점수 평균을 계산합니다.
            """)

        # 키워드 분석 결과 표시
        st.subheader("가장 많이 언급된 키워드 Top 10")
        try:
            # JSON 형태로 저장된 키워드 데이터를 파싱
            top_keywords = json.loads(latest_report['top_keywords'])
            st.table(pd.DataFrame(top_keywords))
        except json.JSONDecodeError:
            st.error("키워드 데이터를 불러오는 데 실패했습니다.")
    else:
        st.warning("처리할 리포트 데이터가 없습니다.")
else:
    st.warning("아직 생성된 일일 리포트가 없습니다. 잠시 후 다시 시도해주세요.")

# --- 실시간 데이터 섹션 ---
st.header("🕒 실시간 데이터 분석")
if not latest_data_df.empty:
    latest_data_df['created_at'] = pd.to_datetime(latest_data_df['created_at'])
    latest_data_df = latest_data_df.sort_values(by="created_at", ascending=False)

    total_tweets = len(latest_data_df)
    
    df_korean = latest_data_df[latest_data_df['detected_language'] == 'ko'].copy()
    df_english = latest_data_df[latest_data_df['detected_language'] == 'en'].copy()
    df_other = latest_data_df[~latest_data_df['detected_language'].isin(['ko', 'en'])].copy()

    col_kpi1, col_kpi2, col_kpi3, col_kpi4 = st.columns(4)
    with col_kpi1:
        st.metric(label="전체 수집 데이터", value=f"{total_tweets:,} 건")
    with col_kpi2:
        st.metric(label="한국어 데이터", value=f"{len(df_korean):,} 건")
    with col_kpi3:
        st.metric(label="영어 데이터", value=f"{len(df_english):,} 건")
    with col_kpi4:
        st.metric(label="기타 언어 데이터", value=f"{len(df_other):,} 건")

    tab1, tab2, tab3 = st.tabs([f"🇰🇷 한국어 ({len(df_korean)}건)", f"🇺🇸 영어 ({len(df_english)}건)", f"🌐 기타 ({len(df_other)}건)"])

    def display_language_dashboard(tab, df, lang_name):
        with tab:
            if not df.empty:
                col1, col2 = st.columns([0.7, 0.3])
                with col1:
                    st.subheader(f"{lang_name} 최신 데이터")
                    display_cols = ['created_at', 'text', 'sentiment_label']
                    if lang_name == "기타 언어":
                        display_cols.append('detected_language')
                    st.dataframe(df[display_cols].head(10))
                with col2:
                    st.subheader("감성 분포")
                    sentiment_counts = df['sentiment_label'].value_counts()
                    st.bar_chart(sentiment_counts)
            else:
                st.info(f"{lang_name} 데이터가 없습니다.")

    display_language_dashboard(tab1, df_korean, "한국어")
    display_language_dashboard(tab2, df_english, "영어")
    display_language_dashboard(tab3, df_other, "기타 언어")

else:
    st.info("수집된 실시간 데이터가 없습니다.")

# 자동 새로고침
st.button("새로고침")