import os
from pyspark.sql import SparkSession
from utils.logger import setup_logger

logger = setup_logger("file_reader")


def read_csv(spark: SparkSession, file_path: str):
    """Reads a CSV file using PySpark with optimizations."""
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return None

    try:
        logger.info(f"Reading CSV file: {file_path}")

        # Load the CSV file
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Log schema and sample data for better insights
        logger.info(f"Data schema: {df.schema}")
        logger.info(f"Sample data: {df.show(false)}")  # Shows first 5 rows for preview

        logger.info("Successfully loaded data!")
        return df

    except Exception as e:
        logger.error(f"Error reading CSV file: {e}", exc_info=True)
        return None
