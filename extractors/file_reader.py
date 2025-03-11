from pyspark.sql import DataFrame
from pyspark.sql import SparkSession

from utils.logger import setup_logger

logger = setup_logger("file_reader")


def read_csv(spark: SparkSession, file_path: str) -> DataFrame:
    """
    Reads a CSV file into a Spark DataFrame.

    :param spark: Spark session
    :param file_path: Path to CSV file
    :return: DataFrame or None if read fails
    """
    try:
        logger.info(f"📂 Reading CSV file: {file_path}")

        # Read CSV with optimized options
        df = spark.read.option("header", True) \
            .option("inferSchema", True) \
            .csv(file_path)

        if df.rdd.isEmpty():
            logger.warning(f"⚠️ CSV file {file_path} is empty. Returning None.")
            return None

        logger.info(f"✅ Successfully read {df.count()} records from {file_path}")
        logger.debug(f"📊 Data Schema: {df.printSchema()}")  # Debug logs
        logger.debug(f"📝 Sample Data:\n{df.show(5, truncate=False)}")

        return df

    except Exception as e:
        logger.error(f"❌ Error reading CSV file: {e}", exc_info=True)
        return None  # Ensure None is returned on failure