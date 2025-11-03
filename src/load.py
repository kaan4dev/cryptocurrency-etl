import os
import logging
from google.cloud import bigquery
import pandas as pd
from dotenv import load_dotenv
from azure.storage.filedatalake import DataLakeServiceClient

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

ACCOUNT_NAME = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
ACCOUNT_KEY = os.getenv("AZURE_STORAGE_ACCOUNT_KEY")
CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME")

RAW_DIR = os.getenv("RAW_DATA", "data/raw")
PROCESSED_DIR = os.getenv("PROCESSED_DATA", "data/processed")
MODELED_DIR = os.getenv("MODELED_DATA", "data/modeled")


def get_service_client():
    try:
        return DataLakeServiceClient(
            account_url=f"https://{ACCOUNT_NAME}.dfs.core.windows.net",
            credential=ACCOUNT_KEY
        )
    except Exception as e:
        logging.error(f"Azure connection failed: {e}")
        raise


def upload_directory(local_dir, remote_dir):
    try:
        service_client = get_service_client()
        file_system_client = service_client.get_file_system_client(CONTAINER_NAME)

        if not os.path.exists(local_dir):
            logging.warning(f"Local directory not found: {local_dir}")
            return

        for root, _, files in os.walk(local_dir):
            for f in files:
                local_path = os.path.join(root, f)
                rel_path = os.path.relpath(local_path, local_dir)
                remote_path = f"{remote_dir}/{rel_path}"

                file_client = file_system_client.get_file_client(remote_path)
                with open(local_path, "rb") as data:
                    file_client.upload_data(data, overwrite=True)

                logging.info(f"Uploaded {f} -> {remote_dir}")

    except Exception as e:
        logging.error(f"Upload failed for {local_dir}: {e}")


def load_to_bigquery():
    client = bigquery.Client(project="crypto-etl-477118")

    dataset_id = "crypto_dataset"
    table_id = "fact_market_snapshot"
    table_ref = f"{dataset_id}.{table_id}"

    dataset_ref = client.dataset(dataset_id)
    try:
        client.get_dataset(dataset_ref)
        print(f"Dataset '{dataset_id}' already exists.")
    except Exception:
        dataset = bigquery.Dataset(f"{client.project}.{dataset_id}")
        dataset.location = "EU"
        client.create_dataset(dataset)
        print(f"Dataset '{dataset_id}' created.")

    modeled_path = os.path.join(MODELED_DIR, "fact_market_summary.parquet")
    df = pd.read_parquet(modeled_path)
    print(f"Loaded {len(df)} rows from {modeled_path}")

    schema = [
        bigquery.SchemaField("symbol", "STRING"),
        bigquery.SchemaField("price_usd", "FLOAT"),
        bigquery.SchemaField("market_cap", "FLOAT"),
        bigquery.SchemaField("volume_24h", "FLOAT"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
    ]

    try:
        client.get_table(table_ref)
        print(f"Table '{table_id}' already exists.")
    except Exception:
        table = bigquery.Table(f"{client.project}.{table_ref}", schema=schema)
        client.create_table(table)
        print(f"Table '{table_id}' created.")

    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema_update_options=["ALLOW_FIELD_ADDITION"]
    )

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()

    print(f"{len(df)} rows successfully loaded into {client.project}.{table_ref}")


if __name__ == "__main__":
    logging.info("Starting Azure upload process...")

    upload_directory(RAW_DIR, "raw")
    upload_directory(PROCESSED_DIR, "processed")
    upload_directory(MODELED_DIR, "modeled")

    load_to_bigquery()

    logging.info("All layers uploaded successfully to Azure Data Lake and BigQuery.")
