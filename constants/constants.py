import os

# 🔹 Application Metadata
APP_NAME = "OptimizedSparkETL"
LOG_LEVEL = "INFO"

# 🔹 Paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE_PATH = os.path.join(BASE_DIR, "config.yaml")
MYSQL_DRIVER_PATH = os.path.join(BASE_DIR, "libs", "mysql-connector-j-8.0.31.jar")
INPUT_FILE_PATH = os.path.join(BASE_DIR, "data", "sample.csv")

# 🔹 Spark Configuration
SPARK_MASTER = "local[*]"
SPARK_SHUFFLE_PARTITIONS = 50
SPARK_ADAPTIVE_EXECUTION = True
SPARK_DRIVER_MEMORY = "4g"
SPARK_EXECUTOR_MEMORY = "8g"

# 🔹 MySQL Configuration
MYSQL_HOST = "localhost"
MYSQL_PORT = "3306"
MYSQL_DB_NAME = "data_db"
MYSQL_USER = "root"
MYSQL_PASSWORD = "root"

# 🔹 JDBC Parameters (Performance Optimized)
JDBC_PARAMS = {
    "serverTimezone": "UTC",
    "useSSL": "false",
    "allowPublicKeyRetrieval": "true",
    "rewriteBatchedStatements": "true",
    "cachePrepStmts": "true",
    "prepStmtCacheSize": "512",
    "prepStmtCacheSqlLimit": "4096",
    "useServerPrepStmts": "true",
    "connectTimeout": "10000",  # 10 seconds
    "socketTimeout": "60000",  # 1 minute
    "autoReconnect": "true"
}

# 🔹 Error Messages
ERROR_CONFIG_FILE_MISSING = f"❌ Config file not found: {CONFIG_FILE_PATH}"
ERROR_MYSQL_DRIVER_MISSING = f"❌ MySQL JDBC Driver not found: {MYSQL_DRIVER_PATH}"
ERROR_FILE_NOT_FOUND = "❌ File not found: {}"
ERROR_EMPTY_DATAFRAME = "❌ DataFrame is empty after transformation."
ERROR_ETL_FAILED = "🔥 ETL Pipeline Failed: {}"

# 🔹 Logging Messages
LOG_START_PIPELINE = "🚀 Starting ETL Pipeline..."
LOG_SPARK_SESSION_INIT = "⚙️ Initializing Spark Session..."
LOG_SPARK_SESSION_STOP = "🛑 Spark Session Stopped."
LOG_FILE_VALIDATED = "📂 Validated File Path: {}"
LOG_EXTRACTED_RECORDS = "📊 Extracted {} records from {}"
LOG_TRANSFORMATION_COMPLETE = "🔄 Transformation complete. Transformed record count: {}"
LOG_DATA_WRITTEN = "🎉 Data successfully written to database!"
