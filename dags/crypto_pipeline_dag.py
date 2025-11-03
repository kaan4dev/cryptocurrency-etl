from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "kaan",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="cryptocurrency_etl_pipeline",
    description="End-to-end ETL for cryptocurrency data using CoinMarketCap, PySpark, and Azure",
    default_args=default_args,
    start_date=datetime(2025, 10, 31),
    schedule_interval="@daily",
    catchup=False,
    tags=["crypto", "etl", "azure"],
) as dag:

    extract = BashOperator(
        task_id="extract_data",
        bash_command="python /opt/airflow/src/extract.py"
    )

    transform = BashOperator(
        task_id="transform_data",
        bash_command="python /opt/airflow/src/transform.py"
    )

    validate = BashOperator(
        task_id="validate_data",
        bash_command="python /opt/airflow/src/validate.py"
    )

    model = BashOperator(
        task_id="model_data",
        bash_command="python /opt/airflow/src/model.py"
    )

    load = BashOperator(
        task_id="load_data",
        bash_command="python /opt/airflow/src/load.py"
    )

    extract >> transform >> validate >> model >> load
