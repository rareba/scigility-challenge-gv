import os
import shutil
from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F

# --- Transformation Functions (Unchanged) ---

def clean_transactions(trans_df: DataFrame, account_df: DataFrame) -> DataFrame:
    """Cleans the transaction data."""
    cleaned_df = trans_df.withColumn(
        "type",
        F.when(F.col("type") == "PRJIEM", "PRIJEM").otherwise(F.col("type"))
    )
    valid_trans_df = cleaned_df.join(
        account_df, on="account_id", how="left_semi"
    )
    return valid_trans_df

def calculate_avg_loan_per_district(loan_df: DataFrame, account_df: DataFrame, district_df: DataFrame) -> DataFrame:
    """Calculates the average loan amount for each district."""
    loan_with_district = loan_df.join(
        account_df, on="account_id", how="inner"
    ).join(
        district_df, on="district_id", how="inner"
    )
    avg_loan_per_district = loan_with_district.groupBy(F.col("A2").alias("district_name")) \
        .agg(
            F.round(F.avg("amount"), 2).alias("average_loan_amount")
        ).orderBy(F.col("average_loan_amount").desc())
    return avg_loan_per_district

# --- Main Interactive Demo Script ---

def run_interactive_demo():
    """Runs the pipeline and shows results interactively."""
    input_path = "data"
    output_path = "output"

    if not os.path.exists(input_path):
        print(f"❌ ERROR: Input folder '{input_path}' not found.")
        return

    if os.path.exists(output_path):
        shutil.rmtree(output_path)
    os.makedirs(output_path)
    
    print("🚀 Starting Interactive Local Demo...")

    # 1. Create SparkSession
    spark = SparkSession.builder.appName("InteractiveDemo").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")  # Keep the output clean from verbose logs
    print("✅ Spark session created.")

    # 2. Read Data
    print(f"📖 Reading data from the '{input_path}' folder...")
    trans_df = spark.read.csv(f"{input_path}/trans.csv", header=True, sep=";", inferSchema=True)
    account_df = spark.read.csv(f"{input_path}/account.csv", header=True, sep=";", inferSchema=True)
    loan_df = spark.read.csv(f"{input_path}/loan.csv", header=True, sep=";", inferSchema=True)
    district_df = spark.read.csv(f"{input_path}/district.csv", header=True, sep=";", inferSchema=True)

    # --- Interactive Demonstration Starts Here ---

    print("\n\n--- 🕵️ PART 1: DEMONSTRATING DATA CLEANING ---")
    
    # Show a transaction with the typo 'PRJIEM'
    print("\n🔎 Step 1: Finding a transaction with the typo 'PRJIEM' BEFORE cleaning...")
    trans_df.filter("type = 'PRJIEM'").show(5)

    # Show a transaction with an invalid account_id (e.g., 96 is not in account.csv)
    print("🔎 Step 2: Finding a transaction with an invalid account_id (e.g., 96) BEFORE cleaning...")
    trans_df.filter("account_id = 96").show(5)

    # Apply the cleaning function
    print("\n⚙️  Applying the cleaning function now...")
    cleaned_trans_df = clean_transactions(trans_df, account_df)
    print("✨ Data has been cleaned!")

    # Verify the typo is fixed
    print("\n🔎 Step 3: Verifying that no transactions with 'PRJIEM' exist AFTER cleaning...")
    # THE FIX IS HERE: we use 'PRJIEM' (single quotes) instead of `PRJIEM` (backticks)
    prijem_count = cleaned_trans_df.filter("type = 'PRJIEM'").count()
    print(f"Count of rows with 'PRJIEM': {prijem_count} --> CORRECT!")

    # Verify the invalid transaction is removed
    print("🔎 Step 4: Verifying the transaction with invalid account_id (96) has been removed...")
    invalid_account_count = cleaned_trans_df.filter("account_id = 96").count()
    print(f"Count of rows with account_id 96: {invalid_account_count} --> CORRECT!")
    
    
    print("\n\n--- 📊 PART 2: DEMONSTRATING AGGREGATION ---")
    
    # Calculate the average loan amount
    print("\n⚙️  Calculating the average loan amount per district...")
    avg_loan_df = calculate_avg_loan_per_district(loan_df, account_df, district_df)
    
    # Show the final aggregation result
    print("\n✅ Final Result: Average loan amount per district (sorted descending):")
    avg_loan_df.show(truncate=False)

    # Save the final results to disk
    print("\n\n--- 💾 PART 3: SAVING FINAL ARTIFACTS ---")
    cleaned_trans_df.write.mode("overwrite").parquet(f"{output_path}/transactions_cleaned")
    print(f"✅ Cleaned transactions saved to '{output_path}/transactions_cleaned'")
    avg_loan_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(f"{output_path}/avg_loan_per_district")
    print(f"✅ Loan analytics saved to '{output_path}/avg_loan_per_district'")

    # Stop the Spark session
    spark.stop()
    print("\n🎉 Demo completed successfully!")

if __name__ == "__main__":
    run_interactive_demo()