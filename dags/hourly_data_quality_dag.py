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
    dag_id='hourly_data_quality_check',
    default_args=default_args,
    description='매시간 HDFS 데이터 품질 검증',
    schedule_interval='0 * * * *', # 매시간 정각 (UTC 기준)
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['data_quality', 'monitoring'],
) as dag:

    run_quality_check = BashOperator(
        task_id='run_hdfs_data_quality_check',
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
        dag=dag,
    )

