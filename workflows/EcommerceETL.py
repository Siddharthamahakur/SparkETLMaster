import logging
import requests
import json
import yaml
import time
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator
from config.config import load_config
from utils.logger import setup_logger

# Load Config
try:
    config = load_config("config.yaml")
    logger = setup_logger("pyspark_optimization")
    logger.info("✅ Configuration loaded successfully.")
except FileNotFoundError:
    logger.error("❌ Config file not found! Exiting...")
    exit(1)

# Initialize Spark Session
try:
    spark = SparkSession.builder \
        .appName(config["spark"]["app_name"]) \
        .config("spark.sql.shuffle.partitions", config["spark"]["shuffle_partitions"]) \
        .config("spark.executor.memory", config["spark"]["executor_memory"]) \
        .config("spark.driver.memory", config["spark"]["driver_memory"]) \
        .config("spark.sql.adaptive.enabled", str(config["spark"]["adaptive_enabled"]).lower()) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("✅ Spark session initialized.")
except Exception as e:
    logger.error("❌ Failed to initialize Spark Session: %s", e)
    raise

MYSQL_URL = config["database"]["jdbc_url"]
TABLE_NAME = config["database"]["table"]
logger.info("✅ MySQL configuration loaded: %s", MYSQL_URL)


def fetch_data(site_url):
    """Fetch product data from an e-commerce site."""
    try:
        start_time = time.time()
        logger.info("🔄 Fetching data from: %s", site_url)
        response = requests.get(site_url, timeout=10)
        response.raise_for_status()
        logger.info("✅ Successfully fetched data from %s in %.2f seconds", site_url, time.time() - start_time)
        return response.json()
    except requests.exceptions.RequestException as e:
        logger.error("❌ Failed to fetch data from %s: %s", site_url, e)
    return []


def extract():
    """Extract data from multiple sources."""
    urls = config.get("api", {}).get("url", [])
    data = [fetch_data(url) for url in urls if url]
    logger.info("📦 Extracted %d records.", sum(len(d) for d in data))
    return [item for sublist in data for item in sublist]


def transform(raw_data):
    """Transform raw data into a structured DataFrame."""
    if not raw_data:
        logger.warning("⚠ No data available for transformation.")
        return spark.createDataFrame([],
                                     schema="id STRING, name STRING, price FLOAT, discount FLOAT, category STRING, site STRING, updated_at TIMESTAMP")

    df = spark.createDataFrame(raw_data).select(
        col("product_id").alias("id"),
        col("name"),
        col("price"),
        col("discount"),
        col("category"),
        col("site"),
        current_timestamp().alias("updated_at")
    )
    df.persist()
    logger.info("🔄 Transformed data with %d records.", df.count())
    return df


def load(df):
    """Load transformed data into MySQL."""
    if df.isEmpty():
        logger.warning("⚠ No new data to load.")
        return
    try:
        engine = create_engine(MYSQL_URL)
        with engine.connect() as conn:
            existing_df = spark.read.format("jdbc").option("url", MYSQL_URL).option("dbtable", TABLE_NAME).load()
            new_records = df.join(existing_df, "id", "left_anti")
            if new_records.count() == 0:
                logger.info("✅ No new records to insert. Database is up-to-date.")
                return
            new_records.write.format("jdbc").option("url", MYSQL_URL).option("dbtable", TABLE_NAME).mode(
                "append").save()
            logger.info("✅ Successfully loaded %d new records into MySQL.", new_records.count())
    except Exception as e:
        logger.exception("❌ Error loading data into MySQL: %s", e)


def etl_pipeline():
    """Run the complete ETL pipeline."""
    logger.info("🚀 Starting ETL pipeline.")
    start_time = time.time()
    raw_data = extract()
    transformed_df = transform(raw_data)
    load(transformed_df)
    logger.info("✅ ETL pipeline completed in %.2f seconds.", time.time() - start_time)


# Airflow DAG Configuration
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 3, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('ecommerce_etl', default_args=default_args, schedule_interval='@daily')

etl_task = PythonOperator(task_id='run_etl', python_callable=etl_pipeline, dag=dag)
logger.info("✅ Airflow DAG setup completed.")
