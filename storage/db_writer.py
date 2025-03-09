from pyspark.sql import DataFrame


class DatabaseHandler:
    def __init__(self, db_url: str, db_user: str, db_password: str):
        self.db_url = db_url
        self.db_user = db_user
        self.db_password = db_password
        self.driver = "com.mysql.cj.jdbc.Driver"

    def write_data(self, df: DataFrame):
        """Writes a PySpark DataFrame to MySQL database."""
        try:
            # Write DataFrame to MySQL directly using Spark's DataFrame API
            df.write \
                .format("jdbc") \
                .option("url", f"jdbc:mysql://{self.db_url}/data_db") \
                .option("driver", self.driver) \
                .option("dbtable", "emp_tbl") \
                .option("user", self.db_user) \
                .option("password", self.db_password) \
                .mode("overwrite") \
                .save()
            print("Data successfully written to MySQL.")
        except Exception as e:
            print(f"Error writing to database: {e}")
