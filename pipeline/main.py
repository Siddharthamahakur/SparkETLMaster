from pyspark.sql import SparkSession
from ingestion.file_reader import read_csv
from processing.transformer import transform_data
from storage.db_writer import DatabaseHandler
from utils.logger import setup_logger

# Set up logger
logger = setup_logger("main")


def run_pipeline(file_path: str):
    """Executes the data pipeline: Read → Transform → Write."""
    file_path = "data/sample.csv"
    spark = None  # Initialize spark session variable

    try:
        logger.info("🚀 Starting the Data Engineering Pipeline...")

        # Initialize Spark Session
        spark = SparkSession.builder \
            .appName("SparkETL") \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.31") \
            .config("spark.sql.shuffle.partitions", "50") \
            .getOrCreate()

        # Read CSV Data
        df = read_csv(spark, file_path)

        # Check if DataFrame is empty
        if df.isEmpty():
            logger.warning(f"⚠️ No data found in {file_path}. Skipping pipeline execution.")
            return

        record_count = df.count()
        logger.info(f"✅ Successfully read {record_count} records from {file_path}")

        # Log schema for debugging
        logger.debug(f"Data Schema:\n{df.printSchema()}")

        # Transform Data
        df_transformed = transform_data(df)

        # Validate transformation
        if df_transformed is None:
            logger.error("❌ Transformation failed. Exiting pipeline.")
            return

        logger.info(f"✅ Data Transformation Successful. Records after transform: {df_transformed.count()}")

        # Write to MySQL
        db_handler = DatabaseHandler(db_url="localhost", db_user="root", db_password="root")
        db_handler.write_data(df_transformed)
        logger.info("🎉 Pipeline Completed Successfully!")

    except Exception as e:
        logger.error(f"🔥 Pipeline execution failed: {e}", exc_info=True)

    finally:
        # Ensure Spark session is stopped to free resources
        if spark:
            spark.stop()
            logger.info("🛑 Spark Session Stopped.")


if __name__ == "__main__":
    run_pipeline("data/sample.csv")
