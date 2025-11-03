import os
import pandas as pd
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID = "crypto-etl-477118"
DATASET_ID = "crypto_dataset"
TABLE_ID = "fact_market_snapshot"

OUTPUT_DIR = "data/analytics"
os.makedirs(OUTPUT_DIR, exist_ok=True)

client = bigquery.Client(project=PROJECT_ID)

queries = {
    "top_10_by_market_cap": f"""
        SELECT symbol, avg_market_cap, avg_price_usd, avg_percent_change_24h
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE avg_market_cap IS NOT NULL
        ORDER BY avg_market_cap DESC
        LIMIT 10
    """,
    "top_gainers": f"""
        SELECT symbol, avg_price_usd, avg_percent_change_24h
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE avg_percent_change_24h IS NOT NULL
        ORDER BY avg_percent_change_24h DESC
        LIMIT 10
    """,
    "avg_price_distribution": f"""
        SELECT
            ROUND(avg_price_usd, 2) AS price_bucket,
            COUNT(*) AS coin_count
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE avg_price_usd > 0
        GROUP BY price_bucket
        ORDER BY price_bucket
    """
}


for name, sql in queries.items():
    df = client.query(sql).to_dataframe()
    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
    df.to_csv(path, index=False)
    print(f"Saved analytics result: {path} ({len(df)} rows)")

# Optional advanced section: price correlation matrix
print("Computing correlation matrix for top 5 coins...")
symbols = ["BTC", "ETH", "BNB", "SOL", "XRP"]
frames = []

for s in symbols:
    q = f"""
        SELECT DATE(timestamp) AS date, AVG(price_usd) AS price_usd
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE symbol = '{s}'
        GROUP BY date
        ORDER BY date
    """
    df = client.query(q).to_dataframe()
    df.rename(columns={"price_usd": s}, inplace=True)
    frames.append(df)

merged = frames[0]
for f in frames[1:]:
    merged = pd.merge(merged, f, on="date", how="outer")

corr = merged[symbols].corr()
corr_path = os.path.join(OUTPUT_DIR, "price_correlation_matrix.csv")
corr.to_csv(corr_path)
print(f"Saved correlation matrix: {corr_path}")
