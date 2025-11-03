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
        SELECT symbol, market_cap, price_usd, volume_24h, timestamp
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE market_cap IS NOT NULL
        ORDER BY market_cap DESC
        LIMIT 10
    """,
    "daily_avg_price": f"""
        SELECT
            DATE(timestamp) AS date,
            AVG(price_usd) AS avg_price_usd
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE price_usd IS NOT NULL
        GROUP BY date
        ORDER BY date
    """,
    "total_market_volume": f"""
        SELECT
            DATE(timestamp) AS date,
            SUM(volume_24h) AS total_volume_24h
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE volume_24h IS NOT NULL
        GROUP BY date
        ORDER BY date
    """,
    "top_gainers": f"""
        SELECT symbol,
               MAX(price_usd) - MIN(price_usd) AS price_diff,
               ROUND(100 * (MAX(price_usd) - MIN(price_usd)) / MIN(price_usd), 2) AS pct_increase
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE price_usd > 0
        GROUP BY symbol
        HAVING COUNT(DISTINCT DATE(timestamp)) > 1
        ORDER BY pct_increase DESC
        LIMIT 10
    """,
    "top_losers": f"""
        SELECT symbol,
               MIN(price_usd) - MAX(price_usd) AS price_diff,
               ROUND(100 * (MIN(price_usd) - MAX(price_usd)) / MAX(price_usd), 2) AS pct_decrease
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE price_usd > 0
        GROUP BY symbol
        HAVING COUNT(DISTINCT DATE(timestamp)) > 1
        ORDER BY pct_decrease ASC
        LIMIT 10
    """,
    "most_volatile": f"""
        SELECT symbol,
               ROUND(STDDEV(price_usd), 4) AS price_stddev,
               ROUND(AVG(price_usd), 4) AS avg_price,
               ROUND((STDDEV(price_usd) / AVG(price_usd)) * 100, 2) AS volatility_pct
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE price_usd > 0
        GROUP BY symbol
        HAVING COUNT(price_usd) > 10
        ORDER BY volatility_pct DESC
        LIMIT 10
    """,
    "market_cap_share": f"""
        WITH latest AS (
            SELECT symbol, market_cap,
                   ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY timestamp DESC) AS rn
            FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
            WHERE market_cap IS NOT NULL
        )
        SELECT
            symbol,
            market_cap,
            ROUND(100 * market_cap / SUM(market_cap) OVER (), 2) AS dominance_pct
        FROM latest
        WHERE rn = 1
        ORDER BY dominance_pct DESC
        LIMIT 10
    """,
    "avg_daily_volume_per_symbol": f"""
        SELECT
            symbol,
            AVG(volume_24h) AS avg_daily_volume
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
        WHERE volume_24h IS NOT NULL
        GROUP BY symbol
        ORDER BY avg_daily_volume DESC
        LIMIT 15
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
