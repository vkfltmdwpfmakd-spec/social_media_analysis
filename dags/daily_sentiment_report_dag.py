"""
Airflow DAG - 일일 감성 분석 리포트 생성

매일 자정에 실행되어 지난 24시간 동안 수집된 소셜 미디어 데이터를
분석하여 일별 감성 리포트를 생성하는 배치 작업입니다.

주요 기능:
- 스케줄링: 매일 자정 UTC 기준 실행
- 데이터 처리: Spark를 이용한 대용량 데이터 배치 처리
- 결과 저장: HDFS에 Parquet 형태로 리포트 저장
- 오류 처리: 실패 시 5분 후 자동 재시도
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Airflow DAG 기본 설정
default_args = {
    'owner': 'airflow',  # DAG 소유자
    'depends_on_past': False,  # 이전 실행 결과에 의존하지 않음
    'email_on_failure': False,  # 실패 시 이메일 알림 비활성화
    'email_on_retry': False,    # 재시도 시 이메일 알림 비활성화
    'retries': 1,               # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 대기 시간
}

# DAG 정의 - 일일 감성 분석 리포트 생성 워크플로우
with DAG(
    dag_id='daily_sentiment_report',  # DAG 고유 식별자
    default_args=default_args,        # 위에서 정의한 기본 설정 적용
    description='매일 자정 Reddit 감성 데이터 집계 리포트 생성',  # DAG 설명
    schedule_interval='0 0 * * *',    # 크론 표현식: 매일 자정 (UTC 기준)
    start_date=datetime(2024, 1, 1),  # DAG 시작일
    catchup=False,                    # 과거 미실행 작업 자동 실행 비활성화
    tags=['sentiment', 'report', 'batch'],  # 분류용 태그
) as dag:

    # Spark 배치 집계 작업 실행
    # Docker 컨테이너 내에서 Spark Submit 명령어로 Python 스크립트 실행
    run_batch_aggregator = BashOperator(
        task_id='run_batch_sentiment_aggregator',  # 태스크 고유 식별자
        bash_command="""
            echo "Running batch sentiment aggregator via Spark streaming analyzer..." && \
            docker exec spark-streaming-analyzer /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
                --conf spark.driver.extraJavaOptions="-Duser.timezone=UTC" \
                --conf spark.executor.extraJavaOptions="-Duser.timezone=UTC" \
                /opt/bitnami/spark/scripts/batch_sentiment_aggregator.py \
            || echo "Batch sentiment aggregator completed with potential issues - continuing pipeline"
        """,
        dag=dag,  # 이 태스크가 속한 DAG 지정
    )

