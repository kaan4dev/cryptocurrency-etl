import os
import logging
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

PROCESSED_DATA = os.getenv("PROCESSED_DATA", "data/processed")
MODELED_DATA = os.getenv("MODELED_DATA", "data/modeled")
os.makedirs(MODELED_DATA, exist_ok=True)

files = [f for f in os.listdir(PROCESSED_DATA) if f.endswith(".parquet")]
if not files:
    raise FileNotFoundError(f"No processed files found in {PROCESSED_DATA}")

latest_file = sorted(files)[-1]
df = pd.read_parquet(os.path.join(PROCESSED_DATA, latest_file))
logging.info(f"Loaded processed dataset: {df.shape}")

summary = (
    df.groupby("symbol")
      .agg(
          avg_price_usd=("price_usd", "mean"),
          avg_market_cap=("market_cap_usd", "mean"),
          avg_percent_change_24h=("percent_change_24h", "mean")
      )
      .reset_index()
)

out_path = os.path.join(MODELED_DATA, "fact_market_summary.parquet")
summary.to_parquet(out_path, index=False)
logging.info(f"Saved modeled dataset -> {out_path}")
