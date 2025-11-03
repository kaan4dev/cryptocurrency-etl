# Cryptocurrency ETL Pipeline

An end-to-end **data engineering pipeline** that extracts live cryptocurrency market data, transforms it for analysis, and loads it into **Azure Data Lake** and **Google BigQuery** for cloud analytics.  
The project also includes automated **Airflow orchestration** and an **analytics layer** that generates CSV reports.

---

## Project Overview

**Goal:** Build a fully automated ETL pipeline that ingests and processes cryptocurrency market data for near real-time insights.

**Features**
- Extracts live crypto listings data (e.g. symbol, price, market cap, volume, timestamp)
- Cleans and transforms data into a modeled Parquet file
- Uploads raw, processed, and modeled data to **Azure Data Lake Gen2**
- Loads aggregated market data into **Google BigQuery**
- Generates analytics CSV reports (top coins, total market volume, daily averages, etc.)
- Managed and orchestrated with **Apache Airflow**

---

## Architecture

```
        +-------------------+
        |  Crypto API (e.g. CoinGecko) |
        +-------------+-----+
                      |
                      v
             [ Extract with Python ]
                      |
                      v
       data/raw → data/processed → data/modeled
                      |
                      v
          +---------------------------+
          |   Azure Data Lake (ADLS)  |
          +---------------------------+
                      |
                      v
          +---------------------------+
          |     Google BigQuery       |
          |  crypto_dataset.fact_market_snapshot |
          +---------------------------+
                      |
                      v
          +---------------------------+
          |  Analytics CSV Reports     |
          |  (Top 10, Daily Avg, etc.)|
          +---------------------------+
```

---

## Tech Stack

| Layer | Technology |
|-------|-------------|
| Orchestration | Apache Airflow (Python DAGs) |
| Data Storage | Azure Data Lake Gen2 |
| Data Warehouse | Google BigQuery |
| Transformation | Pandas, PySpark |
| Validation | Great Expectations *(optional)* |
| Visualization | CSV + Looker Studio / Power BI |
| Environment | Python 3.13, `.env` configuration |

---