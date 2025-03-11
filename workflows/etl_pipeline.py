import os

from pyspark.sql import SparkSession

from extractors.file_reader import read_csv
from loads.db_writer import DatabaseHandler
from transformation.transformer import transform_data
from utils.logger import setup_logger

# Initialize Logger
logger = setup_logger("etl_pipeline")


def setup_spark_session(mysql_driver_path: str) -> SparkSession:
    """
    Initializes and returns a Spark session with optimized configurations.
    Ensures the MySQL JDBC driver is available.
    """
    logger.info("⚙️ Initializing Spark Session...")

    if not os.path.exists(mysql_driver_path):
        logger.error(f"❌ MySQL JDBC Driver not found at: {mysql_driver_path}")
        raise FileNotFoundError(f"MySQL JDBC Driver missing at {mysql_driver_path}")

    spark = (SparkSession.builder
             .appName("OptimizedSparkETL")
             .config("spark.jars", mysql_driver_path)
             .config("spark.sql.shuffle.partitions", "50")  # Optimized shuffle partitions
             .config("spark.sql.adaptive.enabled", "true")  # Enable Adaptive Query Execution (AQE)
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")  # Reduce logs to only errors

    # Log all Spark Configurations
    spark_conf = spark.sparkContext.getConf().getAll()
    logger.info("✅ Spark Session Initialized Successfully.")
    for key, value in spark_conf:
        logger.debug(f"⚙️ {key} = {value}")  # Detailed Spark Config Logs

    return spark


def validate_file(file_path: str) -> str:
    """
    Validates if the given file path exists and returns its absolute path.
    """
    abs_path = os.path.abspath(file_path)
    if not os.path.exists(abs_path):
        logger.error(f"❌ File not found: {abs_path}")
        raise FileNotFoundError(f"File not found: {abs_path}")
    return abs_path


def get_jdbc_url(db_host: str, db_port: str, db_name: str) -> str:
    """
    Constructs and returns an optimized MySQL JDBC URL.
    Ensures proper formatting and best practices for connection settings.
    """
    params = {
        "serverTimezone": "UTC",
        "useSSL": "false",
        "allowPublicKeyRetrieval": "true",
        "rewriteBatchedStatements": "true",
        "cachePrepStmts": "true",
        "prepStmtCacheSize": "512",  # Increased cache size for better performance
        "prepStmtCacheSqlLimit": "4096",  # Increased limit to avoid repeated parsing
        "useServerPrepStmts": "true",  # Enabling server-side prepared statements
        "connectTimeout": "10000",  # Set connection timeout (10 seconds)
        "socketTimeout": "60000",  # Set socket timeout (1 minute)
        "autoReconnect": "true"  # Ensures auto-reconnect in case of failure
    }

    # Construct and return the formatted JDBC URL
    jdbc_url = f"jdbc:mysql://{db_host}:{db_port}/{db_name}?" + "&".join([f"{k}={v}" for k, v in params.items()])

    logger.info(f"🔗 Optimized JDBC URL Constructed: {jdbc_url}")
    return jdbc_url


def run(file_path: str, mysql_driver_path: str):
    """
    Executes the ETL Pipeline:
    1. Extracts data from CSV into a Spark DataFrame.
    2. Transforms the DataFrame using predefined logic.
    3. Loads the transformed data into MySQL using JDBC.
    """
    spark = None

    # Database connection parameters
    db_config = {
        "host": "localhost",
        "port": "3306",
        "name": "data_db",
        "user": "root",
        "password": "root"
    }

    # Construct JDBC URL
    db_url = get_jdbc_url(db_config["host"], db_config["port"], db_config["name"])

    try:
        logger.info("🚀 Starting ETL Pipeline...")

        # Validate File Path
        file_path = validate_file(file_path)
        logger.info(f"📂 Validated File Path: {file_path}")

        # Initialize Spark Session
        spark = setup_spark_session(mysql_driver_path)

        # Extract: Read CSV Data
        df = read_csv(spark, file_path)
        if df is None or df.rdd.isEmpty():
            logger.warning(f"⚠️ No valid data found in {file_path}. Skipping execution.")
            return

        record_count = df.count()
        logger.info(f"📊 Extracted {record_count} records from {file_path}")
        df.printSchema()

        # Transform: Apply transformations
        df_transformed = transform_data(df)
        if df_transformed is None or df_transformed.rdd.isEmpty():
            logger.error("❌ Transformation resulted in an empty DataFrame. Exiting.")
            return

        transformed_count = df_transformed.count()
        logger.info(f"🔄 Transformation complete. Transformed record count: {transformed_count}")
        df_transformed.printSchema()

        # Load: Write transformed data to MySQL
        db_handler = DatabaseHandler(db_url=db_url, db_user=db_config["user"], db_password=db_config["password"])
        db_handler.write_data(df_transformed)

        logger.info("🎉 Data Successfully Written to Database!")

    except FileNotFoundError as fe:
        logger.error(f"📌 File Error: {fe}")

    except Exception as e:
        logger.error(f"🔥 ETL Pipeline Failed: {e}", exc_info=True)

    finally:
        if spark:
            spark.stop()
            logger.info("🛑 Spark Session Stopped.")


if __name__ == "__main__":
    mysql_driver_path = os.path.abspath("libs/mysql-connector-j-8.0.31.jar")
    file_path = os.path.abspath("data/sample.csv")
    run(file_path, mysql_driver_path)
