from pyspark.sql import SparkSession

def get_spark_session(app_name: str, account_name: str, account_key: str) -> SparkSession:
    """
    Creates and configures a SparkSession to connect to Azure Blob Storage.
    
    Args:
        app_name: The name of the Spark application.
        account_name: The Azure Storage account name.
        account_key: The access key for the Azure Storage account.
        
    Returns:
        A configured SparkSession object.
    """
    spark_conf_key = f"fs.azure.account.key.{account_name}.blob.core.windows.net"
    
    spark = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.0.0,org.wildfly.openssl:wildfly-openssl:1.1.3.Final")
        .config(spark_conf_key, account_key)
        .getOrCreate()
    )
    return spark