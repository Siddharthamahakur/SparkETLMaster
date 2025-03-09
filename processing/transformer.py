from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import split

from utils.logger import setup_logger

logger = setup_logger("transformer")


def transform_data(df: DataFrame) -> Optional[DataFrame]:
    try:
        logger.info("Applying transformations to the DataFrame...")

        # Rename 'name' to 'full_name' and split it into first and last name
        df_transformed = df.withColumnRenamed("name", "full_name") \
            .withColumn("first_name", split("full_name", " ")[0]) \
            .withColumn("last_name", split("full_name", " ")[1])

        logger.info("Transformation completed successfully!")
        return df_transformed

    except Exception as e:
        # Log the error with additional context
        logger.error("Error occurred while transforming data", e.getMessage().toString())
        return None
