from pyspark.sql import SparkSession
from ingestion.file_reader import read_csv
from processing.transformer import transform_data
from storage.db_writer import DatabaseHandler
from utils.logger import setup_logger

# Set up logger
logger = setup_logger("main")

def run_pipeline(file_path: str):
    """
    Runs the complete data pipeline: Read → Transform → Write.

    :param file_path: Path to the input CSV file
    """
    spark = None  # Initialize spark session variable

    try:
        logger.info("🚀 Starting Data Engineering Pipeline...")

        # Initialize Spark with optimized configurations
        spark = SparkSession.builder \
            .appName("SparkETL") \
            .config("spark.jars.packages", "mysql:mysql-connector-java:8.0.31") \
            .config("spark.sql.shuffle.partitions", "50") \
            .getOrCreate()

        # Read CSV Data
        df = read_csv(spark, file_path)

        if df is None:
            logger.error("❌ DataFrame is None. Skipping pipeline execution.")
            return

        if df.rdd.isEmpty():
            logger.warning(f"⚠️ No data found in {file_path}. Skipping execution.")
            return

        logger.info(f"📊 Data loaded successfully: {df.count()} records.")

        # Transform Data
        df_transformed = transform_data(df)

        if df_transformed is None:
            logger.error("❌ Transformation failed. Exiting pipeline.")
            return

        logger.info(f"✅ Data Transformation Complete: {df_transformed.count()} records.")

        # Write to MySQL
        db_handler = DatabaseHandler(db_url="localhost", db_user="root", db_password="root")
        db_handler.write_data(df_transformed)
        logger.info("🎉 Pipeline Execution Successful!")

    except Exception as e:
        logger.error(f"🔥 Pipeline execution failed: {e}", exc_info=True)

    finally:
        if spark:
            spark.stop()
            logger.info("🛑 Spark Session Stopped.")

if __name__ == "__main__":
    run_pipeline("data/sample.csv")