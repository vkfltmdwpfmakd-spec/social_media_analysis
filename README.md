# 실시간 소셜 미디어 감성 분석 파이프라인

Reddit API에서 실시간으로 데이터를 수집해서 감성 분석을 수행하고, 결과를 HDFS에 저장한 뒤 대시보드로 시각화하는 데이터 파이프라인입니다. HuggingFace 모델을 사용한 감성 분석과 키워드 기반 폴백 시스템을 구현했습니다.

## 주요 기능

### 감성 분석
- HuggingFace Transformers 모델을 우선 사용하여 높은 정확도 확보
- 모델 오류 시 키워드 기반 분석으로 자동 전환
- 최종 폴백으로 NEUTRAL 처리하여 시스템 안정성 보장

### 다국어 지원
- 정규표현식 패턴을 사용한 언어 감지 (한국어, 영어, 중국어, 일본어, 아랍어, 러시아어, 태국어)
- 언어별 감성 분석 키워드 적용

### 데이터 처리
- Apache Spark Streaming으로 실시간 데이터 처리
- Kafka를 통한 안정적인 메시지 전송
- HDFS에 Parquet 형태로 데이터 저장

### 시스템 운영
- Docker Compose로 전체 인프라 관리
- Airflow로 배치 작업 스케줄링
- 중앙화된 설정 파일로 환경 관리

## 2. 아키텍처

```
Reddit API → Kafka → Spark Streaming → HDFS → Dashboard
    ↓              ↓            ↓           ↓
  데이터 수집    메시지 큐    감성 분석    저장소    시각화
  (실시간)       (스트림)    (다국어)    (Parquet)  (Streamlit)
```

**데이터 흐름:**
1. **Reddit Producer** - Reddit API에서 게시글/댓글 수집
2. **Kafka** - 실시간 데이터 스트림 전송
3. **Spark Streaming** - 언어 감지 + 감성 분석 처리
4. **HDFS** - 분석 결과를 Parquet 파일로 저장
5. **Dashboard** - 실시간 감성 분석 결과 시각화

**감성 분석 과정:**
- HuggingFace 모델 우선 사용 → 실패시 키워드 기반 → 최종 NEUTRAL

**배치 작업 (Airflow):**
- 일일 감성 분석 리포트 생성
- 데이터 품질 검증
- 서비스 상태 모니터링

## 기술 스택

**데이터 처리**
- Apache Spark (Streaming)
- Apache Kafka 
- HDFS

**감성 분석**
- HuggingFace Transformers
- 정규표현식 기반 언어 감지
- Pandas UDF

**인프라 및 운영**
- Docker & Docker Compose
- Apache Airflow
- Streamlit

**개발 및 테스트**
- Python 3.x
- Unittest

## 프로젝트 구조

```
social_media_analysis/
├── config/                     # 설정 파일
│   ├── settings.py
│   ├── logging_config.py
│   └── timezone_utils.py
├── dags/                       # Airflow DAG
│   ├── daily_sentiment_report_dag.py
│   ├── hourly_data_quality_dag.py
│   └── daily_service_monitor_dag.py
├── scripts/                    # 핵심 로직
│   ├── reddit_producer.py
│   ├── sentiment_analyzer_v2.py      # 메인 감성 분석기
│   ├── advanced_sentiment_analyzer.py # HuggingFace 모델
│   └── data_validator.py
├── tests/                      
│   └── test_dags.py
├── dashboard.py                # Streamlit 대시보드
├── docker-compose.yml          
├── Dockerfile                  
├── Dockerfile.spark            
├── requirements.txt            
└── README.md                   
```

## 실행 방법

1. **환경 설정**
   - [Reddit 개발자 페이지](https://www.reddit.com/prefs/apps/)에서 API 키 발급
   - `.env` 파일 생성:
   ```bash
   REDDIT_CLIENT_ID=your_client_id
   REDDIT_CLIENT_SECRET=your_client_secret
   REDDIT_USERNAME=your_username
   REDDIT_PASSWORD=your_password
   REDDIT_USER_AGENT=your_app_name
   ```

2. **실행**
   ```bash
   docker-compose up -d --build
   ```

3. **서비스 접속**
   - Airflow UI: `http://localhost:8082` (admin/admin)
   - Spark UI: `http://localhost:8080`
   - HDFS UI: `http://localhost:9870`
   - Dashboard: `http://localhost:8501`

4. **종료**
   ```bash
   docker-compose down
   ```

## 테스트

```bash
docker exec -it airflow-webserver bash
python -m unittest discover /opt/airflow/tests
```