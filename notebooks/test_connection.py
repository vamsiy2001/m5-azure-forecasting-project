from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql import SparkSession

KEY_VAULT_URL = "https://keyvaultForecasting.vault.azure.net/"
STORAGE_ACCOUNT = "storageforecasting"

credential = DefaultAzureCredential()
kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
storage_key = kv_client.get_secret("adls-primary-key").value

spark = (
    SparkSession.builder
    .appName("m5-local-test")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.5.0")
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", storage_key)
    .getOrCreate()
)

df = spark.read.option("header", True).csv(
    f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/2026-07-16/calendar.csv"
)
df.show(5)
print("Row count:", df.count())