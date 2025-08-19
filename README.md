# 실시간 소셜 미디어 감성 분석 파이프라인 (v2)

이 프로젝트는 Reddit API를 통해 실시간으로 데이터를 수집하고, 다국어(한국어/영어) 감성 분석을 수행하여 HDFS에 저장 및 시각화하는 데이터 파이프라인입니다. v2에서는 설정 중앙화, 테스트 코드 추가, 코드 리팩토링을 통해 안정성과 유지보수성을 대폭 향상했습니다.

## 1. 핵심 개선 사항

- **설정 중앙화:** 모든 주요 설정(경로, 토픽, 모델 이름 등)을 `config/settings.py` 파일에서 관리하여 변경이 용이해졌습니다.
- **테스트 도입:** `tests` 디렉토리를 추가하고 Airflow DAG 무결성 테스트를 도입하여 파이프라인의 안정성을 강화했습니다.
- **코드 리팩토링:** 모든 스크립트가 중앙 설정 파일을 참조하도록 수정하여 코드의 일관성과 가독성을 높였습니다.
- **파일 정리:** 불필요한 파일을 제거하고 `.gitignore`를 추가하여 버전 관리를 체계화했습니다.

## 2. 아키텍처

```
┌───────────────┐   ┌──────────┐   ┌───────────────────────────────────────────┐   ┌───────────────┐   ┌───────────────┐
│               │   │          │   │                                           │   │               │   │               │
│  Reddit API   ├─► │  Kafka   ├─► │      Spark Streaming (Language Detection) ├─► │ Parquet Files ├─► │   Dashboard   │
│               │   │          │   │ (KoELECTRA for KO / DistilBERT for EN) │   │   (HDFS)      │   │  (Streamlit)  │
│               │   │          │   │                                           │   │               │   │               │
└───────────────┘   └──────────┘   └───────────────────────────────────────────┘   └───────────────┘   └───────────────┘

# Airflow: 매일 리포트 생성, 매시간 데이터 품질 검증, 서비스 상태 모니터링 등 배치성 작업을 오케스트레이션합니다.
```

- **데이터 수집/전송:** `PRAW`를 사용하여 Reddit API로부터 데이터를 수집하고 `Kafka`로 전송합니다. (`scripts/reddit_producer.py`)
- **데이터 처리/분석:** `Spark Streaming`이 Kafka로부터 데이터를 소비하여 언어를 감지하고, 적절한 Hugging Face 모델로 감성 분석을 수행합니다. 결과는 `Parquet` 포맷으로 HDFS에 저장됩니다. (`scripts/sentiment_analyzer.py`)
- **워크플로우 관리:** `Apache Airflow`가 일일 리포트 생성, 데이터 품질 검증, 서비스 상태 모니터링 DAG를 스케줄에 따라 실행합니다. (`dags/`)
- **시각화:** `Streamlit`을 사용하여 분석된 데이터를 실시간으로 시각화합니다. (`dashboard.py`)

## 3. 기술 스택

- **Orchestration:** Apache Airflow
- **Containerization:** Docker, Docker Compose
- **Message Queue:** Apache Kafka
- **Data Processing:** Apache Spark (Spark Streaming & Batch)
- **Data Lake Storage:** HDFS
- **Language Detection:** fastText
- **Sentiment Analysis:** Hugging Face Transformers (KoELECTRA, DistilBERT)
- **Visualization:** Streamlit
- **Testing:** Pytest (예정), Unittest

## 4. 프로젝트 구조

```
social_media_analysis/
├── config/               # 중앙 설정 관리
│   └── settings.py
├── dags/                 # Airflow DAG 파일
│   ├── daily_sentiment_report_dag.py
│   ├── hourly_data_quality_dag.py
│   └── daily_service_monitor_dag.py
├── scripts/              # 핵심 로직 스크립트
│   ├── reddit_producer.py
│   ├── sentiment_analyzer.py
│   └── ...
├── tests/                # 테스트 코드
│   └── test_dags.py
├── .env                  # 환경 변수 설정 파일
├── .gitignore            # 버전 관리 제외 파일 목록
├── dashboard.py          # Streamlit 대시보드
├── docker-compose.yml    # 서비스 및 인프라 정의
├── Dockerfile            # Airflow 커스텀 이미지 빌드
├── Dockerfile.spark      # Spark 커스텀 이미지 빌드
├── requirements.txt      # Python 의존성 목록
├── README.md             # 프로젝트 설명 (현재 파일)
└── PROJECT_ANALYSIS.md   # 프로젝트 상세 분석 문서 (NEW)
```

## 5. 사용법

1.  **`.env` 파일 설정:**
    - [Reddit 개발자 페이지](https://www.reddit.com/prefs/apps/)에서 'script' 타입의 앱을 생성합니다.
    - `.env` 파일에 발급받은 `client_id`, `client_secret`, `username`, `password`, `user_agent`를 입력합니다.

2.  **서비스 실행:**
    ```bash
    docker-compose up -d --build
    ```

3.  **서비스 확인:**
    - **Airflow UI:** `http://localhost:8082` (ID/PW: admin/admin)
    - **Spark Master UI:** `http://localhost:8080`
    - **HDFS Namenode UI:** `http://localhost:9870`
    - **Dashboard:** `http://localhost:8501`

4.  **테스트 실행 (선택 사항):**
    - Airflow 컨테이너에 접속하여 테스트를 실행할 수 있습니다.
    ```bash
    docker exec -it airflow-webserver bash
    python -m unittest discover /opt/airflow/tests
    ```

## 6. 종료 방법

```bash
docker-compose down
```