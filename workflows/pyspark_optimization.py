import os
import time
import traceback

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, broadcast
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

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
        .config("spark.sql.adaptive.enabled", str(config["spark"]["adaptive_enabled"]).lower()) \
        .config("spark.sql.shuffle.partitions", str(config["spark"]["shuffle_partitions"])) \
        .config("spark.sql.files.maxPartitionBytes", config["spark"]["max_partition_bytes"]) \
        .config("spark.executor.memory", config["spark"]["executor_memory"]) \
        .config("spark.executor.cores", str(config["spark"]["executor_cores"])) \
        .config("spark.driver.memory", config["spark"]["driver_memory"]) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")  # Suppress excessive logs
    logger.info("✅ Spark Session initialized successfully.")

except Exception as e:
    logger.error(f"❌ Failed to initialize Spark Session: {e}\n{traceback.format_exc()}")
    exit(1)

# Define Schema
schema = StructType([
    StructField("id", StringType(), True),
    StructField("category", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

# Load Data
try:
    start_time = time.time()
    logger.info("📂 Loading raw data from local path...")

    df = spark.read.schema(schema).json(config["dataset"]["raw_data_path"])
    record_count = df.count()

    if record_count == 0:
        logger.warning("⚠️ No records found in raw data. Exiting...")
        exit(1)

    logger.info(f"✅ Loaded {record_count} records from raw data.")

except Exception as e:
    logger.error(f"❌ Failed to load raw data: {e}\n{traceback.format_exc()}")
    exit(1)

# Apply Transformations
try:
    logger.info("📌 Applying partitioning strategy based on year...")
    df = df.withColumn("year", col("timestamp").substr(1, 4))
    logger.info("✅ Partitioning strategy applied.")
except Exception as e:
    logger.error(f"❌ Error applying transformations: {e}\n{traceback.format_exc()}")
    exit(1)

# Load Dimension Table
dim_path = config["dataset"]["dimension_data_path"]

try:
    if os.path.exists(dim_path) and os.listdir(dim_path):
        logger.info("🔍 Loading and broadcasting dimension table...")

        dim_df = spark.read.parquet(dim_path)

        # Handle duplicate columns before joining
        dim_df = dim_df.drop("value")  # Drop conflicting column
        dim_df = broadcast(dim_df)

        logger.info("✅ Dimension table broadcasted successfully.")
    else:
        logger.warning(f"⚠️ Dimension table path {dim_path} is empty or does not exist.")
        dim_df = spark.createDataFrame([], schema)

except Exception as e:
    logger.error(f"❌ Failed to load dimension table: {e}\n{traceback.format_exc()}")
    exit(1)

# Join & Repartition
try:
    logger.info("🔄 Applying filters and joining with dimension table...")
    filtered_df = df.filter(col("value") > 1000).select("id", "category", "value", "year")

    # Aliasing the columns to avoid ambiguity during the join
    enriched_df = filtered_df.join(dim_df.alias("dim"), filtered_df["id"] == dim_df["id"], "left") \
        .select(filtered_df["id"], filtered_df["category"], filtered_df["value"], filtered_df["year"],
                dim_df["category"].alias("dim_category"))

    # Repartition to optimize writes
    logger.info("📊 Repartitioning DataFrame to optimize write performance...")
    final_df = enriched_df.repartition("year", "dim_category")

    logger.info("✅ Transformations and joins completed successfully.")
except Exception as e:
    logger.error(f"❌ Error during joins and transformations: {e}\n{traceback.format_exc()}")
    exit(1)

# Write Output
output_path = config["dataset"]["output_data_path"]
try:
    logger.info(f"🚀 Writing processed data to {output_path}...")

    final_df.write.partitionBy("year", "dim_category").mode("overwrite").parquet(output_path)

    elapsed_time = time.time() - start_time
    logger.info(f"✅ Data successfully written in {elapsed_time:.2f} seconds.")

except Exception as e:
    logger.error(f"❌ Failed to write output data: {e}\n{traceback.format_exc()}")
    exit(1)

# Stop Spark Session
spark.stop()
logger.info("🛑 Spark Session Stopped. ETL process completed successfully.")
