from pyspark.sql import DataFrame

from utils.logger import setup_logger

logger = setup_logger("db_writer")


class DatabaseHandler:
    def __init__(self, db_url: str, db_user: str, db_password: str):
        """
        Initializes the DatabaseHandler for writing data to MySQL.

        :param db_url: MySQL JDBC connection URL
        :param db_user: Database username
        :param db_password: Database password
        """
        try:
            if not db_url or not db_user or not db_password:
                raise ValueError("Database URL, username, and password must be provided.")

            self.db_url = db_url
            self.db_properties = {
                "user": db_user,
                "password": db_password,
                "driver": "com.mysql.cj.jdbc.Driver"
            }

            logger.info("✅ DatabaseHandler initialized successfully.")
        except Exception as e:
            logger.error(f"❌ Failed to initialize DatabaseHandler: {e}", exc_info=True)
            raise

    def write_data(self, df: DataFrame, table_name: str = "processed_table"):
        """
        Writes a Spark DataFrame to MySQL using JDBC.

        :param df: Transformed DataFrame
        :param table_name: Target MySQL table (default: 'processed_table')
        """
        try:
            if df is None or df.rdd.isEmpty():
                raise ValueError("DataFrame is empty. Skipping write operation.")

            record_count = df.count()
            logger.info(f"📤 Writing {record_count} records to MySQL table `{table_name}`...")

            df.write.jdbc(url=self.db_url, table=table_name, mode="append", properties=self.db_properties)

            logger.info(f"✅ Successfully wrote {record_count} records to `{table_name}`.")
        except ValueError as ve:
            logger.warning(f"⚠️ {ve}")
        except Exception as e:
            logger.error(f"❌ Error writing data to MySQL: {e}", exc_info=True)
            raise
