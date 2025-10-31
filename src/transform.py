import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, round, current_timestamp
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

RAW_DATA = os.getenv("RAW_DATA", "data/raw")
PROCESSED_DATA = os.getenv("PROCESSED_DATA", "data/processed")
os.makedirs(PROCESSED_DATA, exist_ok=True)

spark = (
    SparkSession.builder
    .appName("Crypto_Transform_Pipeline")
    .config("spark.sql.session.timeZone", "UTC")
    .getOrCreate()
)

logging.info("Spark session started")

csv_files = [f for f in os.listdir(RAW_DATA) if f.startswith("crypto_listings_") and f.endswith(".csv")]

if not csv_files:
    raise FileNotFoundError(f"No crypto_listings CSV files found in {RAW_DATA}")

latest_file = sorted(csv_files)[-1]

raw_path = os.path.join(RAW_DATA, latest_file)
out_path = os.path.join(PROCESSED_DATA, f"processed_{latest_file}")

df = spark.read.option("header", True).csv(raw_path, inferSchema=True)
logging.info(f"Loaded {df.count()} rows, {len(df.columns)} columns")

logging.info(f"Reading: {raw_path}")

df = (
    df
    .filter(col("price").isNotNull())
    .withColumn("price_usd", round(col("price"), 4))
    .withColumn("market_cap_usd", round(col("market_cap"), 2))
    .withColumn("volume_24h_usd", round(col("volume_24h"), 2))
    .drop("price", "market_cap", "volume_24h")
    .withColumnRenamed("cmc_rank", "rank")
    .withColumn("load_timestamp", current_timestamp())
)

(df.repartition(1).write.mode("overwrite").parquet(out_path.replace(".csv", ".parquet")))

logging.info(f"Saved processed dataset -> {out_path.replace('.csv', '.parquet')}")
logging.info(f"Final schema: {len(df.columns)} columns")

df.show(10, truncate=False)
