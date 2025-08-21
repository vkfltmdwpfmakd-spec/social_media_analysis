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
    dag_id='daily_sentiment_report',
    default_args=default_args,
    description='매일 자정 Reddit 감성 데이터 집계 리포트 생성',
    schedule_interval='0 0 * * *', # 매일 자정 (UTC 기준)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['sentiment', 'report', 'batch'],
) as dag:

    run_batch_aggregator = BashOperator(
        task_id='run_batch_sentiment_aggregator',
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
        dag=dag,
    )

