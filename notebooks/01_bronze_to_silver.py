from azure.identity import DefaultAzureCredential
from azure.keyvault.secrets import SecretClient
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

KEY_VAULT_URL = "https://keyvaultForecasting.vault.azure.net/"
STORAGE_ACCOUNT = "storageforecasting"
BRONZE_DATE = "2026-07-16"  # match your real bronze partition folder

credential = DefaultAzureCredential()
kv_client = SecretClient(vault_url=KEY_VAULT_URL, credential=credential)
storage_key = kv_client.get_secret("adls-primary-key").value

spark = (
    SparkSession.builder
    .appName("m5-bronze-to-silver")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.5.0")
    .config(f"fs.azure.account.key.{STORAGE_ACCOUNT}.dfs.core.windows.net", storage_key)
    .config("spark.driver.memory", "6g")
    .config("spark.sql.shuffle.partitions", "8")
    .getOrCreate()
)

BASE = f"abfss://bronze@{STORAGE_ACCOUNT}.dfs.core.windows.net/{BRONZE_DATE}"

# --- Load sales (wide format: one row per item-store, one column per day) ---
sales_wide = spark.read.option("header", True).csv(f"{BASE}/sales_train_evaluation.csv")

print("Sales row count (should be 30,490 -- one per item-store series):", sales_wide.count())
print("Column count:", len(sales_wide.columns))
sales_wide.select(sales_wide.columns[:8]).show(3)

id_cols = ["id", "item_id", "dept_id", "cat_id", "store_id", "state_id"]
day_cols = [c for c in sales_wide.columns if c.startswith("d_")]

# sample = sales_wide.filter(F.col("store_id") == "CA_1")

sales_long = sales_wide.unpivot(
    ids=id_cols,
    values=day_cols,
    variableColumnName="d",
    valueColumnName="units_sold",
).withColumn("units_sold", F.col("units_sold").cast("int"))\
 .withColumn("d_num", F.regexp_extract(F.col("d"), r"d_(\d+)", 1).cast("int"))

# print("Melted sample row count (should be 5 items x 1941 days):", sample_long.count())
# sample_long.orderBy("id", "d").show(10)

sales_long.filter(F.col("id") == "HOBBIES_1_001_CA_1_evaluation").orderBy("d_num").show(15)

print("Full melted row count (should be ~59,181,090 -- 30,490 x 1,941):", sales_long.count())

sales_long.write.mode("overwrite").partitionBy("state_id").parquet(
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales_long/"
)

calendar = spark.read.option("header", True).csv(f"{BASE}/calendar.csv")
sell_prices = spark.read.option("header", True).csv(f"{BASE}/sell_prices.csv")

sales_with_dates = sales_long.join(
    calendar.select("d", "date", "wm_yr_wk", "event_name_1", "event_type_1",
                     "snap_CA", "snap_TX", "snap_WI"),
    on="d", how="left"
)

sales_enriched = sales_with_dates.join(
    sell_prices,
    on=["store_id", "item_id", "wm_yr_wk"],
    how="left"
)

print("Enriched row count:", sales_enriched.count())  # should still be 59,181,090 — left joins don't drop rows

sales_enriched.write.mode("overwrite").partitionBy("state_id").parquet(
    f"abfss://silver@{STORAGE_ACCOUNT}.dfs.core.windows.net/sales_enriched/"
)

spark.stop()