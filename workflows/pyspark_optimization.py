import os
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

from utils.logger import setup_logger

# Setup Logger
logger = setup_logger("pyspark_optimization")

try:
    logger.info("Initializing Spark Session...")
    spark = SparkSession.builder \
        .appName("Optimized ETL Pipeline") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.shuffle.partitions", "500") \
        .config("spark.sql.files.maxPartitionBytes", "256MB") \
        .config("spark.executor.memory", "8g") \
        .config("spark.executor.cores", "4") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
    logger.info("Spark Session Initialized Successfully")

    # Define Schema for Raw Data
    schema = StructType([
        StructField("id", StringType(), True),
        StructField("category", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("timestamp", StringType(), True)
    ])

    # Load Raw Data
    start_time = time.time()
    raw_data_path = "data/raw-data/*.json"
    logger.info(f"Loading raw data from {raw_data_path}...")
    df = spark.read.schema(schema).json(raw_data_path)
    record_count = df.count()
    logger.info(f"Successfully loaded {record_count} records from raw data")

    # Data Partitioning Strategy
    logger.info("Applying partitioning strategy based on year")
    df = df.withColumn("year", col("timestamp").substr(1, 4))

    # Define Schema for Dimension Table
    dim_schema = StructType([
        StructField("id", StringType(), True),
        StructField("dim_category", StringType(), True),
        StructField("extra_info", StringType(), True)
    ])

    # Load Dimension Table with Validation
    dim_table_path = "data/dimensions"
    if os.path.exists(dim_table_path) and len(os.listdir(dim_table_path)) > 0:
        logger.info(f"Loading dimension table from {dim_table_path}...")
        dim_df = spark.read.schema(dim_schema).parquet(dim_table_path)
        dim_df = broadcast(dim_df)
        logger.info("Dimension table loaded and broadcasted successfully")
    else:
        logger.warning(f"Dimension table path {dim_table_path} is empty or does not exist. Using an empty DataFrame.")
        dim_df = spark.createDataFrame([], dim_schema)  # Empty DataFrame

    # Transformations and Joins
    logger.info("Applying transformations and filtering records...")
    filtered_df = df.filter(col("value") > 1000).select("id", "category", "value", "year")
    logger.info("Joining raw data with dimension table...")
    enriched_df = filtered_df.join(dim_df, "id", "left")
    logger.info("Transformations and joins completed successfully")

    # Repartition Data for Optimized Writing
    logger.info("Repartitioning data before writing to disk...")
    final_df = enriched_df.repartition("year", "category")

    # Write Data to Local Path
    output_path = "data/processed-data/"
    logger.info(f"Writing processed data to {output_path}...")
    final_df.write \
        .partitionBy("year", "category") \
        .mode("overwrite") \
        .parquet(output_path)
    logger.info("Data successfully written to local path")

    elapsed_time = time.time() - start_time
    logger.info(f"ETL Completed in {elapsed_time:.2f} seconds")

except Exception as e:
    logger.error(f"ETL Pipeline Failed: {e}\n{traceback.format_exc()}")

finally:
    if 'spark' in locals():
        spark.stop()
        logger.info("Spark Session Stopped")