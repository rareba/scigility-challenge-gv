import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    Pytest fixture to create a local SparkSession for testing.
    Tears down the session after tests are complete.
    """
    session = (
        SparkSession.builder.master("local[2]")
        .appName("pytest-local-spark")
        .getOrCreate()
    )
    yield session
    session.stop()