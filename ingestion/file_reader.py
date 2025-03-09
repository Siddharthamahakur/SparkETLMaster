from pyspark.sql import SparkSession

from utils.logger import setup_logger

logger = setup_logger("file_reader")


def read_csv(file_path):
    """Reads a CSV file using PySpark"""
    try:
        spark = SparkSession.builder.appName("DataIngestion").getOrCreate()
        logger.info(f"Reading file: {file_path}")
        df = spark.read.csv(file_path, header=True, inferSchema=True)
        logger.info("Successfully loaded data!")
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}", exc_info=True)
        return None
