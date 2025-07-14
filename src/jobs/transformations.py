from pyspark.sql import DataFrame
import pyspark.sql.functions as F

def clean_transactions(trans_df: DataFrame, account_df: DataFrame) -> DataFrame:
    """
    Cleans the transaction data by fixing a typo and filtering for valid accounts.

    - Fixes typo: "PRJIEM" should be "PRIJEM".
    - Filters out transactions where account_id does not exist in the account table.
    """
    # 1. Fix the typo in the 'type' column
    cleaned_df = trans_df.withColumn(
        "type",
        F.when(F.col("type") == "PRJIEM", "PRIJEM").otherwise(F.col("type"))
    )

    # 2. Filter for transactions with a valid account using a semi-join
    # This is efficient as it only filters and doesn't add columns.
    valid_trans_df = cleaned_df.join(
        account_df, on="account_id", how="left_semi"
    )
    
    return valid_trans_df

def calculate_avg_loan_per_district(loan_df: DataFrame, account_df: DataFrame, district_df: DataFrame) -> DataFrame:
    """
    Calculates the average loan amount for each district.

    - Joins loan, account, and district tables.
    - Groups by district name.
    - Calculates the average loan amount.
    """
    # Join the tables to link loans to districts
    loan_with_district = loan_df.join(
        account_df, on="account_id", how="inner"
    ).join(
        district_df, on="district_id", how="inner"
    )

    # Group by district name (A2) and calculate the average loan amount
    avg_loan_per_district = loan_with_district.groupBy(F.col("A2").alias("district_name")) \
        .agg(
            F.round(F.avg("amount"), 2).alias("average_loan_amount")
        ).orderBy("district_name")

    return avg_loan_per_district