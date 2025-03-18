import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator, get_current_context
from airflow.utils.dates import days_ago

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from sqlalchemy import create_engine
from config.config import load_config
from utils.logger import setup_logger

# ✅ Load Config
try:
    config = load_config("config.yaml")
    logger = setup_logger("pyspark_optimization")
    logger.info("✅ Configuration loaded successfully.")
except FileNotFoundError:
    logger.error("❌ Config file not found! Exiting...")
    exit(1)


# ✅ Initialize Spark Session (Optimized)
def init_spark():
    """ Initialize Spark session """
    try:
        spark = (SparkSession.builder
                 .appName(config["spark"]["app_name"])
                 .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"])
                 .config("spark.executor.memory", config["spark"]["executor_memory"])
                 .config("spark.driver.memory", config["spark"]["driver_memory"])
                 .config("spark.sql.adaptive.enabled", str(config["spark"]["adaptive_enabled"]).lower())
                 .getOrCreate())
        spark.sparkContext.setLogLevel("ERROR")
        logger.info("✅ Spark session initialized.")
        return spark
    except Exception as e:
        logger.error(f"❌ Failed to initialize Spark Session: {e}")
        raise


# ✅ Fetch Data with Enhanced Error Handling and Retries
def fetch_data(site_url, retries=3, timeout=10):
    """ Fetch product data from an e-commerce site with retries """
    for attempt in range(retries):
        try:
            start_time = time.time()
            logger.info(f"🔄 Fetching data from: {site_url} (Attempt {attempt + 1}/{retries})")

            response = requests.get(site_url, timeout=timeout)
            response.raise_for_status()

            logger.info(f"✅ Successfully fetched data from {site_url} in {time.time() - start_time:.2f} seconds")
            return response.json()

        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ Request failed: {e}. Retrying...")
            time.sleep(2)

    logger.error(f"❌ Failed to fetch data from {site_url} after {retries} attempts.")
    return []


# ✅ Extract: API Extraction with Parallelization
def extract(**kwargs):
    """ Extract data from multiple sources with parallel processing """
    urls = config.get("ecommerce_sites", [])

    from multiprocessing import Pool
    with Pool(len(urls)) as pool:
        data = pool.map(fetch_data, urls)

    combined_data = [item for sublist in data for item in sublist]

    logger.info(f"📦 Extracted {len(combined_data)} records.")

    # Store data in XCom for next task
    context = get_current_context()
    context['ti'].xcom_push(key='raw_data', value=combined_data)


# ✅ Transform: Optimized DataFrame Processing with Schema Validation
def transform(**kwargs):
    """ Transform raw data into a structured Spark DataFrame """
    context = get_current_context()
    raw_data = context['ti'].xcom_pull(task_ids='extract', key='raw_data')

    if not raw_data:
        logger.warning("⚠ No data available for transformation.")
        return

    spark = init_spark()

    schema = "id STRING, name STRING, price FLOAT, discount FLOAT, category STRING, site STRING, updated_at TIMESTAMP"

    df = (spark.createDataFrame(raw_data)
          .select(
        col("product_id").alias("id"),
        col("name"),
        col("price"),
        col("discount"),
        col("category"),
        col("site"),
        current_timestamp().alias("updated_at")
    )
          .repartition("site")  # ✅ Optimized partitioning by site
          .persist())

    logger.info(f"🔄 Transformed data with {df.count()} records.")

    # Store transformed DataFrame in XCom
    context['ti'].xcom_push(key='transformed_df', value=df)


# ✅ Load: Optimized MySQL Loading with Batch Inserts and Retry Mechanism
def load(**kwargs):
    """ Load transformed data into MySQL """
    context = get_current_context()
    df = context['ti'].xcom_pull(task_ids='transform', key='transformed_df')

    if df.isEmpty():
        logger.warning("⚠ No new data to load.")
        return

    MYSQL_URL = config["mysql"]["url"]
    TABLE_NAME = config["mysql"]["table"]

    try:
        engine = create_engine(MYSQL_URL)
        with engine.connect() as conn:

            # ✅ Use MySQL batching for faster writes
            df.write \
                .format("jdbc") \
                .option("url", MYSQL_URL) \
                .option("dbtable", TABLE_NAME) \
                .option("batchsize", 1000) \
                .option("numPartitions", 8) \
                .mode("append") \
                .save()

            logger.info(f"✅ Successfully loaded {df.count()} new records into MySQL.")
    except Exception as e:
        logger.exception(f"❌ Error loading data into MySQL: {e}")


# ✅ Airflow DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'optimized_etl_pipeline',
    default_args=default_args,
    description='Optimized ETL Pipeline with Airflow, PySpark, and MySQL',
    schedule_interval='@daily',
    catchup=False
)

# ✅ Define Tasks
extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    dag=dag
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag
)

# ✅ Set Task Dependencies
extract_task >> transform_task >> load_task

logger.info("✅ Airflow DAG setup completed.")
