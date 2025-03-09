from pyspark.sql import SparkSession

from processing.transformer import transform_data
from storage.db_writer import DatabaseHandler
from utils.logger import setup_logger

# Set up logger
logger = setup_logger("main")


def run_pipeline(file_path: str):
    """Executes the data pipeline: Read → Transform → Write."""
    try:
        logger.info("Starting the Data Engineering Pipeline...")

        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("SparkETL") \
            .config("spark.jars", "libs/mysql-connector-j-8.0.31.jar") \
            .getOrCreate()

        # Read CSV Data using Spark
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Check if DataFrame is empty
        if df.isEmpty():
            logger.warning(f"No data found in {file_path}. Skipping pipeline execution.")
            return

        record_count = df.count()
        logger.info(f"Successfully read {record_count} records from {file_path}")

        # Transform Data
        df_transformed = transform_data(df)

        # Write to MySQL directly using Spark's DataFrame API (jdbc)
        # Example usage in your main script
        db_handler = DatabaseHandler(db_url="localhost", db_user="root", db_password="root")
        db_handler.write_data(df_transformed)

        logger.info("Pipeline Completed Successfully!")

    except Exception as e:
        logger.error(f"Pipeline execution failed: {e}", exc_info=True)


if __name__ == "__main__":
    run_pipeline("data/sample.csv")
