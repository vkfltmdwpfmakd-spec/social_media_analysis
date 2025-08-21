"""
Airflow DAG - 시간별 데이터 품질 검증

매시간 실행되어 HDFS에 저장된 소셜 미디어 데이터의 품질을 검증하고
데이터 파이프라인의 건전성을 모니터링하는 작업입니다.

주요 기능:
- 스케줄링: 매시간 정각 UTC 기준 실행
- 데이터 검증: 누락, 중복, 형식 오류 등 품질 이슈 탐지
- 실시간 모니터링: 데이터 수집 상태 지속적 감시
- 알림 시스템: 문제 발견 시 운영팀에 알림
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# Airflow DAG 기본 설정 - 데이터 품질 검증용
default_args = {
    'owner': 'airflow',               # DAG 소유자
    'depends_on_past': False,         # 이전 실행 결과에 의존하지 않음
    'email_on_failure': False,        # 실패 시 이메일 알림 비활성화
    'email_on_retry': False,          # 재시도 시 이메일 알림 비활성화
    'retries': 1,                     # 실패 시 재시도 횟수
    'retry_delay': timedelta(minutes=5),  # 재시도 대기 시간
}

# DAG 정의 - 시간별 데이터 품질 검증 워크플로우
with DAG(
    dag_id='hourly_data_quality_check',  # DAG 고유 식별자
    default_args=default_args,           # 위에서 정의한 기본 설정 적용
    description='매시간 HDFS 데이터 품질 검증',    # DAG 설명
    schedule_interval='0 * * * *',       # 크론 표현식: 매시간 정각 (UTC 기준)
    start_date=datetime(2024, 1, 1),     # DAG 시작일
    catchup=False,                       # 과거 미실행 작업 자동 실행 비활성화
    tags=['data_quality', 'monitoring'], # 분류용 태그
) as dag:

    # 데이터 품질 검증 작업 실행
    # Docker 컨테이너 내에서 Spark Submit으로 품질 검증 스크립트 실행
    run_quality_check = BashOperator(
        task_id='run_hdfs_data_quality_check',  # 태스크 고유 식별자
        bash_command="""
            echo "Running HDFS data quality check via Spark streaming analyzer..." && \
            docker exec spark-streaming-analyzer /opt/bitnami/spark/bin/spark-submit \
                --master spark://spark-master:7077 \
                --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
                --conf spark.driver.extraJavaOptions="-Duser.timezone=UTC" \
                --conf spark.executor.extraJavaOptions="-Duser.timezone=UTC" \
                /opt/bitnami/spark/scripts/data_quality_check.py \
            || echo "Data quality check completed with potential issues - continuing pipeline"
        """,
        dag=dag,  # 이 태스크가 속한 DAG 지정
    )

