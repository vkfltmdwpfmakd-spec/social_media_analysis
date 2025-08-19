from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='daily_service_monitor',
    default_args=default_args,
    description='매일 핵심 서비스(Kafka, Spark, HDFS) 상태 모니터링',
    schedule_interval='30 0 * * *', # 매일 0시 30분 (UTC 기준)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['monitoring', 'health_check'],
) as dag:

    run_service_check = BashOperator(
        task_id='run_service_health_check',
        bash_command=f"""
            python /opt/airflow/scripts/service_health_check.py
        """,
        dag=dag,
    )
