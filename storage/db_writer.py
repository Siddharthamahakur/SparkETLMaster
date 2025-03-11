from pyspark.sql import DataFrame
from utils.logger import setup_logger

logger = setup_logger("db_writer")


class DatabaseHandler:
    def __init__(self, db_url: str, db_user: str, db_password: str):
        """
        Initializes the DatabaseHandler class.

        :param db_url: MySQL connection URL
        :param db_user: Database username
        :param db_password: Database password
        """
        self.db_url = f"jdbc:mysql://{db_url}:3306/data_db"
        self.db_user = db_user
        self.db_password = db_password
        self.db_properties = {
            "user": self.db_user,
            "password": self.db_password,
            "driver": "com.mysql.cj.jdbc.Driver"
        }

    def write_data(self, df: DataFrame):
        """
        Writes DataFrame to MySQL table.

        :param df: Transformed DataFrame
        """
        try:
            logger.info(f"📤 Writing {df.count()} records to MySQL database...")
            df.write.jdbc(url=self.db_url, table="processed_table", mode="append", properties=self.db_properties)
            logger.info("✅ Data successfully written to MySQL!")
        except Exception as e:
            logger.error(f"❌ Error writing data to MySQL: {e}", exc_info=True)