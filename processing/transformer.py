from pyspark.sql import DataFrame
from pyspark.sql.functions import split
from utils.logger import setup_logger

logger = setup_logger("transformer")


def transform_data(df: DataFrame) -> DataFrame:
    """
    Transforms the input DataFrame by renaming columns and splitting names.

    :param df: Input DataFrame
    :return: Transformed DataFrame
    """
    try:
        logger.info("🔄 Applying transformations to DataFrame...")

        df_transformed = df.withColumnRenamed("name", "full_name") \
            .withColumn("first_name", split("full_name", " ")[0]) \
            .withColumn("last_name", split("full_name", " ")[1])

        logger.info(f"✅ Transformation complete! Total records: {df_transformed.count()}")
        return df_transformed

    except Exception as e:
        logger.error(f"❌ Error transforming data: {e}", exc_info=True)
        return None  # Ensure None is returned on failure