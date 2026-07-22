from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KEY_VAULT_URL = "https://keyvaultForecasting.vault.azure.net/"
STORAGE_ACCOUNT = "storageforecasting"

credential = DefaultAzureCredential()
kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
storage_key = kv_client.get_secret("adls-primary-key").value

spark = (
    SparkSession.builder
    .appName("m5-silver-to-gold")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.5.0")
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", storage_key)
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

sales_enriched = spark.read.parquet(
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales_enriched/"
)

sales_typed = sales_enriched.withColumn(
    "snap_active",
    F.when(F.col("state_id") == "CA", F.col("snap_CA"))
     .when(F.col("state_id") == "TX", F.col("snap_TX"))
     .when(F.col("state_id") == "WI", F.col("snap_WI"))
     .otherwise(None).cast("int")
).withColumn("sell_price", F.col("sell_price").cast("double"))

gold_daily_category = (
    sales_typed.groupBy("date", "state_id", "store_id", "cat_id")
    .agg(
        F.sum("units_sold").alias("total_units_sold"),
        F.avg("sell_price").alias("avg_sell_price"),
        F.max("snap_active").alias("snap_active"),
        F.max(F.when(F.col("event_name_1").isNotNull(), 1).otherwise(0)).alias("had_event"),
    )
    .orderBy("date", "store_id", "cat_id")
)

print("Gold row count (expect 58,230 = 10 stores x 3 cats x 1,941 days):", gold_daily_category.count())

gold_daily_category.write.mode("overwrite").parquet(
    f"abfss://gold@{STORAGE_ACCOUNT}.dfs.core.windows.net/daily_sales_by_store_category/"
)

gold_daily_category.toPandas().to_csv("gold_daily_sales_by_store_category.csv", index=False)

spark.stop()
