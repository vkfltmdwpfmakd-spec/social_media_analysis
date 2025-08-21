import streamlit as st
import pandas as pd
import json
import logging
import os
import io
from hdfs import InsecureClient
from datetime import timedelta

# --- 환경 변수 및 기본 설정 ---
HDFS_NAMENODE_HOST = os.getenv('HDFS_NAMENODE_HOST', 'namenode')
HDFS_WEB_PORT = int(os.getenv('HDFS_WEB_PORT', 9870))
DASHBOARD_REFRESH_SECONDS = int(os.getenv('DASHBOARD_REFRESH_SECONDS', 30))

# HDFS 경로
HDFS_DATA_PATH = "/user/spark/social_sentiment_data"
REPORT_PATH = "/user/spark/daily_sentiment_reports"

# HDFS 클라이언트 초기화
client = InsecureClient(f'http://{HDFS_NAMENODE_HOST}:{HDFS_WEB_PORT}', user='root')

# 로거 설정
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- 데이터 로딩 함수 ---
def load_hdfs_data(hdfs_path):
    """HDFS에서 모든 Parquet 파일을 읽어 Pandas DataFrame으로 반환합니다."""
    try:
        if not client.status(hdfs_path, strict=False):
            logger.warning(f"HDFS 경로를 찾을 수 없습니다: {hdfs_path}")
            return pd.DataFrame()

        file_list = client.list(hdfs_path)
        parquet_files = [f for f in file_list if f.endswith('.parquet')]
        
        if not parquet_files:
            return pd.DataFrame()

        all_dfs = []
        for file_name in parquet_files:
            full_path = os.path.join(hdfs_path, file_name)
            with client.read(full_path) as reader:
                buffer = io.BytesIO(reader.read())
                df = pd.read_parquet(buffer)
                all_dfs.append(df)
        
        if not all_dfs:
            return pd.DataFrame()
            
        return pd.concat(all_dfs, ignore_index=True)

    except Exception as e:
        logger.error(f"HDFS 데이터 로딩 중 오류 발생: {e}", exc_info=True)
        st.error(f"HDFS에서 데이터를 로드하는 중 오류가 발생했습니다: {e}")
        return pd.DataFrame()

# --- Streamlit UI 구성 ---
st.set_page_config(layout="wide")
st.title("소셜 미디어 감성 분석 대시보드")

# --- 데이터 로딩 ---
report_df = load_hdfs_data(REPORT_PATH)
latest_data_df = load_hdfs_data(HDFS_DATA_PATH)

# --- 일일 리포트 섹션 ---
st.header("📊 일일 감성 분석 리포트")
if not report_df.empty:
    report_df['report_date'] = pd.to_datetime(report_df['report_date'])
    report_df = report_df.sort_values(by='report_date', ascending=False)
    
    if not report_df.empty:
        latest_report = report_df.iloc[0]
        
        kst_time = latest_report['report_date'] + timedelta(hours=9)

        col1, col2 = st.columns(2)
        with col1:
            st.metric("리포트 생성 시간 (KST)", kst_time.strftime('%Y-%m-%d %H:%M:%S'))
        with col2:
            st.metric("평균 감성 점수", f"{latest_report['average_sentiment_score']:.4f}")
        
        with st.expander("ℹ️ 평균 감성 점수 산정 기준 보기"):
            st.markdown("""
            - **Positive:** +1점
            - **Negative:** -1점
            - **Neutral (또는 기타):** 0점
            
            위 기준으로 지난 24시간 동안 수집된 모든 데이터의 감성 점수 평균을 계산합니다.
            """)

        st.subheader("가장 많이 언급된 키워드 Top 10")
        try:
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