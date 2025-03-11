from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import split, col, when
from pyspark.sql.utils import AnalysisException
from utils.logger import setup_logger

# Setup logger
logger = setup_logger("transformer")

def transform_data(df: DataFrame) -> Optional[DataFrame]:
    """
    Transforms the input DataFrame by renaming the 'name' column to 'full_name'
    and splitting it into 'first_name' and 'last_name'.

    :param df: Input PySpark DataFrame
    :return: Transformed PySpark DataFrame or None if an error occurs
    """
    try:
        logger.info("Starting data transformation...")

        # Ensure the 'name' column exists in the DataFrame
        required_columns = {"name"}
        df_columns = set(df.columns)

        if not required_columns.issubset(df_columns):
            logger.error(f"Missing required columns: {required_columns - df_columns}")
            return None

        # Apply transformations safely
        df_transformed = df.withColumnRenamed("name", "full_name") \
            .withColumn("first_name", when(col("full_name").isNotNull(), split(col("full_name"), " ")[0]).otherwise(None)) \
            .withColumn("last_name", when(col("full_name").isNotNull(), split(col("full_name"), " ")[1]).otherwise(None))

        logger.info("Transformation completed successfully!")
        return df_transformed

    except AnalysisException as e:
        logger.error(f"AnalysisException encountered: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error during transformation: {str(e)}")

    return None  # Return None if an error occurs