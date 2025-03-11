import os

import yaml
from pyspark.sql import SparkSession

from utils.logger import setup_logger

# Load Logger
logger = setup_logger("settings")


class ConfigLoader:
    """Loads configuration from a YAML file for the PySpark application."""

    def __init__(self, config_path: str = "config.yaml"):
        try:
            if not os.path.exists(config_path):
                raise FileNotFoundError(f"⚠️ Config file not found at: {config_path}")

            with open(config_path, "r") as file:
                self.config = yaml.safe_load(file)

            logger.info("✅ Configuration file loaded successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to load configuration: {e}", exc_info=True)
            raise

    def get(self, key_path: str, default=None):
        """
        Retrieves a value from the nested configuration using dot notation.

        :param key_path: Key path in dot notation (e.g., 'database.host')
        :param default: Default value if the key is missing
        :return: The value or default if not found
        """
        keys = key_path.split(".")
        value = self.config
        try:
            for key in keys:
                value = value[key]
            return value
        except KeyError:
            logger.warning(f"⚠️ Config key '{key_path}' not found. Using default: {default}")
            return default


def setup_spark_session(config: ConfigLoader) -> SparkSession:
    """
    Initializes and returns a Spark session with configurations.

    :param config: ConfigLoader instance
    :return: SparkSession object
    """
    logger.info("⚙️ Initializing Spark Session...")

    mysql_driver_path = config.get("paths.mysql_driver")

    if not os.path.exists(mysql_driver_path):
        logger.error(f"❌ MySQL JDBC Driver not found at: {mysql_driver_path}")
        raise FileNotFoundError(f"MySQL JDBC Driver missing at {mysql_driver_path}")

    spark = (SparkSession.builder
             .appName(config.get("app.name", "OptimizedSparkETL"))
             .master(config.get("spark.master", "local[*]"))
             .config("spark.jars", mysql_driver_path)
             .config("spark.sql.shuffle.partitions", str(config.get("spark.shuffle_partitions", 50)))
             .config("spark.sql.adaptive.enabled", str(config.get("spark.adaptive_execution", "true")).lower())
             .config("spark.driver.memory", config.get("spark.driver_memory", "4g"))
             .config("spark.executor.memory", config.get("spark.executor_memory", "8g"))
             .getOrCreate())

    logger.info("✅ Spark Session Initialized Successfully.")

    # Log Spark Configurations
    for key, value in spark.sparkContext.getConf().getAll():
        logger.debug(f"⚙️ {key} = {value}")

    return spark


def get_jdbc_url(config: ConfigLoader) -> str:
    """
    Constructs an optimized MySQL JDBC URL from the config.

    :param config: ConfigLoader instance
    :return: Formatted JDBC URL
    """
    host = config.get("database.host")
    port = config.get("database.port")
    db_name = config.get("database.name")

    jdbc_params = config.get("database.jdbc_params", {})
    jdbc_url = f"jdbc:mysql://{host}:{port}/{db_name}?" + "&".join([f"{k}={v}" for k, v in jdbc_params.items()])

    logger.info(f"🔗 Optimized JDBC URL Constructed: {jdbc_url}")
    return jdbc_url
