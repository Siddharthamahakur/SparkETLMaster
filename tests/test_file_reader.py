import pytest
from pyspark.sql import SparkSession

from ingestion.file_reader import read_csv


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("TestSession").getOrCreate()


def test_read_csv(spark):
    df = read_csv("tests/test_data.csv")
    assert df is not None, "Dataframe should not be None"
