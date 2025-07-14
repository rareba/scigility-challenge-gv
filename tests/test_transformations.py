import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from src.jobs.transformations import clean_transactions, calculate_avg_loan_per_district

def test_clean_transactions(spark: SparkSession):
    """
    Tests the clean_transactions function.
    """
    # Schema definitions
    trans_schema = StructType([
        StructField("trans_id", IntegerType()),
        StructField("account_id", IntegerType()),
        StructField("type", StringType())
    ])
    account_schema = StructType([
        StructField("account_id", IntegerType()),
        StructField("district_id", IntegerType())
    ])

    # Create test data
    trans_data = [(1, 101, "PRIJEM"), (2, 101, "PRJIEM"), (3, 999, "VYDAJ")] # 999 is an invalid account
    account_data = [(101, 1), (102, 2)]

    trans_df = spark.createDataFrame(trans_data, trans_schema)
    account_df = spark.createDataFrame(account_data, account_schema)

    # Apply the function
    result_df = clean_transactions(trans_df, account_df)
    results = result_df.collect()

    # Assertions
    assert result_df.count() == 2
    assert results[0]["type"] == "PRIJEM"
    assert results[1]["type"] == "PRIJEM" # Check if typo was corrected
    assert set(row.account_id for row in results) == {101} # Check if invalid account was filtered

def test_calculate_avg_loan_per_district(spark: SparkSession):
    """
    Tests the calculate_avg_loan_per_district function.
    """
    # Schema definitions
    loan_schema = StructType([StructField("account_id", IntegerType()), StructField("amount", DoubleType())])
    account_schema = StructType([StructField("account_id", IntegerType()), StructField("district_id", IntegerType())])
    district_schema = StructType([StructField("district_id", IntegerType()), StructField("A2", StringType())])

    # Create test data
    loan_data = [(101, 1000.0), (102, 2000.0), (103, 3000.0)]
    account_data = [(101, 1), (102, 1), (103, 2)]
    district_data = [(1, "Prague"), (2, "Brno")]

    loan_df = spark.createDataFrame(loan_data, loan_schema)
    account_df = spark.createDataFrame(account_data, account_schema)
    district_df = spark.createDataFrame(district_data, district_schema)
    
    # Apply the function
    result_df = calculate_avg_loan_per_district(loan_df, account_df, district_df)
    results = {row.district_name: row.average_loan_amount for row in result_df.collect()}

    # Assertions
    assert result_df.count() == 2
    assert results["Prague"] == 1500.0 # (1000 + 2000) / 2
    assert results["Brno"] == 3000.0