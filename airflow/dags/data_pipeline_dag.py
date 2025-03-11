import json
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.providers.apache.kafka.operators.consume import KafkaConsumeMessagesOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator

from airflow import DAG
from extractors.file_reader import read_csv
from loads.db_writer import write_to_mysql
from transformation.transformer import transform_data
from utils.logger import setup_logger

# Setup Logger
logger = setup_logger("airflow_dag")

# Airflow Default Arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 10),
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

# Initialize DAG
dag = DAG(
    "data_pipeline_dag",
    default_args=default_args,
    schedule_interval="@daily",  # Runs daily
    catchup=False,
)

# 🔹 Kafka Consumer Task
consume_kafka_task = KafkaConsumeMessagesOperator(
    task_id="consume_kafka_messages",
    kafka_topic="data_topic",
    kafka_bootstrap_servers="localhost:9092",
    consumer_group_id="airflow-consumer-group",
    auto_offset_reset="earliest",  # Start from the beginning if no offset exists
    dag=dag,
)


# 🔹 Read, Transform, and Pass Data via XCom
def read_transform_and_store(**kwargs):
    """Reads CSV data, transforms it, and stores the result in XCom."""
    try:
        df = read_csv("/path/to/data/sample.csv")
        if df is None:
            raise ValueError("Failed to read CSV data")

        df_transformed = transform_data(df)

        if df_transformed is None:
            raise ValueError("Transformation returned None")

        # Serialize DataFrame as JSON (safe for XCom)
        data_json = json.dumps(df_transformed.toPandas().to_dict(orient="records"))

        kwargs['ti'].xcom_push(key='transformed_data', value=data_json)
        logger.info("Data transformed and stored in XCom successfully.")
    except Exception as e:
        logger.error(f"Error in read_transform_and_store: {e}", exc_info=True)
        raise


read_transform_and_store_task = PythonOperator(
    task_id="read_transform_and_store",
    python_callable=read_transform_and_store,
    provide_context=True,
    dag=dag,
)


# 🔹 Write Transformed Data to MySQL
def write_to_db_from_xcom(**kwargs):
    """Fetches transformed data from XCom and writes it to MySQL."""
    try:
        data_json = kwargs['ti'].xcom_pull(task_ids='read_transform_and_store', key='transformed_data')
        if not data_json:
            raise ValueError("No transformed data found in XCom.")

        data_list = json.loads(data_json)  # Convert JSON back to list of dicts

        # Batch Insert Optimization
        write_to_mysql(data_list, "processed_table", batch_size=1000)
        logger.info("Data successfully written to MySQL.")
    except Exception as e:
        logger.error(f"Error in write_to_db_from_xcom: {e}", exc_info=True)
        raise


write_to_mysql_task = PythonOperator(
    task_id="write_to_mysql",
    python_callable=write_to_db_from_xcom,
    provide_context=True,
    dag=dag,
)


# 🔹 Spark Job Execution
spark_task = SparkSubmitOperator(
    task_id="run_spark_job",
    application="/path/to/spark_job.py",
    conn_id="spark_default",
    conf={
        "spark.executor.memory": "2g",
        "spark.driver.memory": "1g",
        "spark.sql.shuffle.partitions": "50",
    },
    dag=dag,
)


# 🔹 Create MySQL Table (If Not Exists)
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


# 🔹 DAG Dependencies
create_table_task >> consume_kafka_task >> read_transform_and_store_task >> write_to_mysql_task >> spark_task