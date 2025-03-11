import logging
import os

import pytest
from pyspark.sql import SparkSession

from extractors.file_reader import read_csv

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark():
    """
    Fixture to create and return a Spark session for testing.
    The session is shared across all test cases to improve efficiency.
    """
    spark_session = SparkSession.builder.master("local").appName("TestSession").getOrCreate()
    yield spark_session
    spark_session.stop()  # Ensure Spark session is properly closed after tests


@pytest.fixture
def temp_csv_file(tmpdir):
    """
    Fixture to create a temporary CSV file for testing.
    The file is automatically deleted after the test session.
    """
    file_path = tmpdir.join("test_data.csv")
    sample_data = "col1,col2\n1,2\n3,4\n5,6"  # Sample CSV content
    file_path.write(sample_data)
    logger.info(f"Temporary CSV file created: {file_path}")
    return str(file_path)


def test_read_csv_if_not_exists(spark, temp_csv_file):
    """
    Test if the read_csv function correctly reads an existing CSV file into a DataFrame.
    """
    logger.info(f"Testing read_csv with file: {temp_csv_file}")

    # Ensure the test file exists before reading
    assert os.path.exists(temp_csv_file), f"Test file does not exist: {temp_csv_file}"

    # Read CSV file into a Spark DataFrame
    df = read_csv(temp_csv_file)

    # Validate the DataFrame is successfully loaded
    assert df is not None, "Expected a DataFrame, but got None"
    assert df.count() > 0, "DataFrame should not be empty"

    logger.info(f"CSV file read successfully. Row count: {df.count()}")