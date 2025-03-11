import logging
import os
from pyspark.sql import DataFrame
from pyspark.sql.utils import AnalysisException
from time import sleep

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseHandler:
    """
    Handles writing PySpark DataFrames to a MySQL database using JDBC.
    """

    def __init__(self, db_url: str, db_user: str, db_password: str, database: str = "data_db"):
        """
        Initializes the database handler with connection parameters.

        :param db_url: MySQL database URL (without database name)
        :param db_user: Database username
        :param db_password: Database password
        :param database: Database name (default: "data_db")
        """
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.database = database
        self.driver = "com.mysql.cj.jdbc.Driver"

    def write_data(self, df: DataFrame, table_name: str, mode: str = "overwrite", max_retries: int = 3):
        """
        Writes a PySpark DataFrame to the MySQL database.

        :param df: PySpark DataFrame to be written
        :param table_name: Target table name in MySQL
        :param mode: Write mode ("overwrite", "append", "error")
        :param max_retries: Number of retry attempts in case of failure
        """
        jdbc_url = f"jdbc:mysql://{self.db_url}/{self.database}"

        if df is None or df.isEmpty():
            logger.warning("Empty DataFrame provided. Skipping write operation.")
            return

        retry_attempts = 0
        while retry_attempts < max_retries:
            try:
                logger.info(f"Writing data to MySQL table: {table_name} (Mode: {mode})")

                # Write DataFrame to MySQL using JDBC
                df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("driver", self.driver) \
                    .option("dbtable", table_name) \
                    .option("user", self.db_user) \
                    .option("password", self.db_password) \
                    .mode(mode) \
                    .save()

                logger.info(f"Data successfully written to {self.database}.{table_name}")
                return  # Exit after successful write

            except AnalysisException as e:
                logger.error(f"Table '{table_name}' does not exist in MySQL. Error: {e}")
                break  # Exit if table is missing

            except Exception as e:
                retry_attempts += 1
                logger.error(f"Error writing to database (Attempt {retry_attempts}/{max_retries}): {e}")

                if retry_attempts < max_retries:
                    sleep(2)  # Wait before retrying
                else:
                    logger.critical(f"Failed to write to {self.database}.{table_name} after {max_retries} attempts.")
                    raise e  # Rethrow error after max retries