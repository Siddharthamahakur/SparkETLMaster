from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import KafkaConsumeMessagesOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from airflow import DAG
from ingestion.file_reader import read_csv
from processing.transformer import transform_data
from storage.db_writer import write_to_mysql
from utils.logger import setup_logger

logger = setup_logger("airflow_dag")

# Define default_args for Airflow DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 10),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Initialize the DAG
dag = DAG(
    "data_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Run once per day
    catchup=False,
)

# Task 1: Consume messages from Kafka
consume_kafka_task = KafkaConsumeMessagesOperator(
    task_id="consume_kafka_messages",
    kafka_topic="data_topic",
    kafka_bootstrap_servers="localhost:9092",
    dag=dag,
)


# Task 2: Read Data from CSV
def read_data():
    df = read_csv("/path/to/data/sample.csv")
    if df:
        df.write.mode("overwrite").parquet("/tmp/processed_data")
        logger.info("Data read successfully and stored as Parquet.")


read_csv_task = PythonOperator(
    task_id="read_csv_data",
    python_callable=read_data,
    dag=dag,
)


# Task 3: Transform Data
def transform_and_store():
    df = read_csv("/tmp/processed_data")
    df_transformed = transform_data(df)
    df_transformed.write.mode("overwrite").parquet("/tmp/final_data")
    logger.info("Data transformation completed.")


transform_data_task = PythonOperator(
    task_id="transform_data",
    python_callable=transform_and_store,
    dag=dag,
)


# Task 4: Write Transformed Data to MySQL
def write_to_db():
    df = read_csv("/tmp/final_data")
    data = [tuple(row) for row in df.collect()]
    write_to_mysql(data, "processed_table")
    logger.info("Data written to MySQL.")


write_to_mysql_task = PythonOperator(
    task_id="write_to_mysql",
    python_callable=write_to_db,
    dag=dag,
)

# Task 5: Spark Job Execution (Example)
spark_task = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/path/to/spark_job.py",
    conn_id="spark_default",
    dag=dag,
)

# Task 6: Create MySQL Table (If Not Exists)
create_table_task = MySqlOperator(
    task_id="create_mysql_table",
    mysql_conn_id="mysql_default",
    sql="""
    CREATE TABLE IF NOT EXISTS processed_table (
        id INT AUTO_INCREMENT PRIMARY KEY,
        full_name VARCHAR(255),
        age INT,
        country VARCHAR(255)
    );
    """,
    dag=dag,
)

# Define DAG Dependencies
create_table_task >> consume_kafka_task >> read_csv_task >> transform_data_task >> spark_task >> write_to_mysql_task
