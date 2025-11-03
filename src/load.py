import os
import logging

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

RAW_DATA = os.getenv("RAW_DATA", "data/raw")
PROCESSED_DATA = os.getenv("PROCESSED_DATA", "data/processed")
MODELED_DATA = os.getenv("MODELED_DATA", "data/modeled")

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
            logging.warning(f"Local directory cannot be found: {local_dir}")
            return 
        
        for root, _, files in os.walk(local_dir):
            for f in files:
                local_path = os.path.join(root,f)
                rel_path = os.path.relpath(local_path, local_dir)
                remote_path = f"{remote_dir}/{rel_path}"

                file_client = file_system_client.get_file_client(remote_path)
                with open(local_path, "rb") as data:
                    file_client.upload_data(data, overwrite = True)

                logging.info(f"Uploaded {f} -> {remote_dir}")
        
    except Exception as e:
        logging.error(f"Upload failed for {local_dir} : {e}")

if __name__ == "__main__":
    logging.info("Starting Azure upload process...")

    upload_directory(RAW_DATA, "raw")
    upload_directory(PROCESSED_DATA, "processed")
    upload_directory(MODELED_DATA, "modeled")

    logging.info("All layers uploaded successfully to Azure Data Lake.")