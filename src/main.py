import os
import sys
from utils.spark import get_spark_session
from jobs.transformations import clean_transactions, calculate_avg_loan_per_district

def main():
    """
    Main entrypoint for the Spark data processing pipeline.
    """
    # Retrieve Azure Storage credentials from environment variables
    # These will be set in the execution environment (e.g., ACI, Databricks, Kubernetes)
    storage_account_name = os.environ.get("AZURE_STORAGE_ACCOUNT_NAME")
    storage_account_key = os.environ.get("AZURE_STORAGE_ACCOUNT_KEY")

    if not all([storage_account_name, storage_account_key]):
        print("Error: AZURE_STORAGE_ACCOUNT_NAME and AZURE_STORAGE_ACCOUNT_KEY environment variables must be set.")
        sys.exit(1)

    # Initialize Spark Session configured for Azure Blob Storage
    spark = get_spark_session("BankDataPipeline", storage_account_name, storage_account_key)
    print("Spark session created successfully.")

    # Define input and output paths
    base_path = f"wasbs://data@{storage_account_name}.blob.core.windows.net"
    input_path = f"{base_path}/raw"
    output_path = f"{base_path}/processed"

    # 1. Read input files from Azure Blob Storage
    try:
        print(f"Reading data from {input_path}...")
        trans_df = spark.read.csv(f"{input_path}/trans.csv", header=True, sep=";", inferSchema=True)
        account_df = spark.read.csv(f"{input_path}/account.csv", header=True, sep=";", inferSchema=True)
        loan_df = spark.read.csv(f"{input_path}/loan.csv", header=True, sep=";", inferSchema=True)
        district_df = spark.read.csv(f"{input_path}/district.csv", header=True, sep=";", inferSchema=True)
        print("All data read successfully.")
    except Exception as e:
        print(f"Failed to read data: {e}")
        spark.stop()
        sys.exit(1)

    # 2. Clean the transactions data
    print("Cleaning transactions data...")
    cleaned_trans_df = clean_transactions(trans_df, account_df)
    
    # 3. Calculate the average loan amount per district
    print("Calculating average loan amount per district...")
    avg_loan_df = calculate_avg_loan_per_district(loan_df, account_df, district_df)

    # 4. Save the results back to Azure Blob Storage
    try:
        # Save cleaned transactions as Parquet
        transactions_output_path = f"{output_path}/transactions"
        print(f"Saving cleaned transactions to {transactions_output_path}")
        cleaned_trans_df.write.mode("overwrite").parquet(transactions_output_path)

        # Save average loan amounts as a single CSV file for easy viewing
        analytics_output_path = f"{output_path}/analytics/avg_loan_per_district"
        print(f"Saving average loan amounts to {analytics_output_path}")
        avg_loan_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(analytics_output_path)
        print("All outputs saved successfully.")
    except Exception as e:
        print(f"Failed to write data: {e}")
    finally:
        # Stop the Spark session
        print("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()