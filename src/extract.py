import os
import json
import logging
import requests
import pandas as pd
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("COINMARKETCAP_API")
HEADERS = {"Accepts": "application/json", "X-CMC_PRO_API_KEY": API_KEY}
BASE_URL = "https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest"

RAW_DATA = os.getenv("RAW_DATA", "data/raw")
os.makedirs(RAW_DATA, exist_ok=True)

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def fetch_crypto_listings(limit=5000, convert="USD"):
    """
    Fetch the list of all active cryptocurrencies with detailed data.
    """
    url = f"{BASE_URL}?limit={limit}&convert={convert}"
    logging.info(f"Requesting up to {limit} coins from CoinMarketCap...")

    response = requests.get(url, headers=HEADERS)
    if response.status_code != 200:
        logging.error(f"Error {response.status_code}: {response.text}")
        return None

    data = response.json().get("data", [])
    logging.info(f"Fetched {len(data)} coins successfully.")

    records = []
    for coin in data:
        usd = coin.get("quote", {}).get("USD", {})
        records.append({
            "id": coin.get("id"),
            "name": coin.get("name"),
            "symbol": coin.get("symbol"),
            "slug": coin.get("slug"),
            "cmc_rank": coin.get("cmc_rank"),
            "date_added": coin.get("date_added"),
            "num_market_pairs": coin.get("num_market_pairs"),
            "circulating_supply": coin.get("circulating_supply"),
            "total_supply": coin.get("total_supply"),
            "max_supply": coin.get("max_supply"),
            "last_updated": usd.get("last_updated"),
            "price": usd.get("price"),
            "volume_24h": usd.get("volume_24h"),
            "market_cap": usd.get("market_cap"),
            "percent_change_1h": usd.get("percent_change_1h"),
            "percent_change_24h": usd.get("percent_change_24h"),
            "percent_change_7d": usd.get("percent_change_7d"),
            "percent_change_30d": usd.get("percent_change_30d"),
            "percent_change_60d": usd.get("percent_change_60d"),
            "percent_change_90d": usd.get("percent_change_90d")
        })

    return records

def save_dataset(records):
    if not records:
        logging.warning("No data to save.")
        return

    df = pd.DataFrame(records)
    ts = pd.Timestamp.utcnow().strftime("%Y%m%d_%H%M%S")

    json_path = os.path.join(RAW_DATA, f"crypto_listings_{ts}.json")
    csv_path = os.path.join(RAW_DATA, f"crypto_listings_{ts}.csv")

    df.to_json(json_path, orient="records", indent=2)
    df.to_csv(csv_path, index=False)

    logging.info(f"Saved JSON -> {json_path}")
    logging.info(f"Saved CSV  -> {csv_path}")
    logging.info(f"Total rows: {len(df)} | Columns: {len(df.columns)}")

    print("\n" + "=" * 100)
    print("DATASET PREVIEW")
    print("=" * 100)
    print(df.head(10).to_string(index=False))
    print("=" * 100 + "\n")

if __name__ == "__main__":
    records = fetch_crypto_listings(limit=5000)
    save_dataset(records)
