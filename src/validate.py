import os
import json
import logging

import pandas as pd
import great_expectations as gx
from great_expectations.core.batch import Batch
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.execution_engine import PandasExecutionEngine
from great_expectations.execution_engine.pandas_batch_data import PandasBatchData
from great_expectations.validator.validator import Validator
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] %(message)s"
)

PROCESSED_DATA = os.getenv("PROCESSED_DATA", "data/processed")
GX_REPORTS_DIR = "outputs/validation_reports"
os.makedirs(GX_REPORTS_DIR, exist_ok=True)

files = [f for f in os.listdir(PROCESSED_DATA) if f.endswith(".parquet")]
if not files:
    raise FileNotFoundError(f"No Parquet files found in {PROCESSED_DATA}")
latest_file = sorted(files)[-1]
data_path = os.path.join(PROCESSED_DATA, latest_file)

logging.info(f"Validating file: {data_path}")

df = pd.read_parquet(data_path)
logging.info(f"Dataset loaded: {len(df)} rows, {len(df.columns)} columns")

context = gx.get_context(mode="ephemeral")
execution_engine = PandasExecutionEngine()

batch_data = PandasBatchData(execution_engine=execution_engine, dataframe=df)
batch = Batch(data=batch_data)
expectation_suite = ExpectationSuite(name="crypto_suite")
context.suites.add_or_update(expectation_suite)
validator = Validator(
    execution_engine=execution_engine,
    data_context=context,
    expectation_suite=expectation_suite,
    batches=[batch],
)

validator.expect_column_to_exist("symbol")
validator.expect_column_values_to_not_be_null("symbol")
validator.expect_column_values_to_be_unique("symbol", mostly=0.6)
validator.expect_column_to_exist("price_usd")
validator.expect_column_values_to_not_be_null("price_usd")
validator.expect_column_values_to_be_between("price_usd", min_value=0)
validator.expect_column_to_exist("market_cap_usd")
validator.expect_column_values_to_be_between("market_cap_usd", min_value=0)
validator.expect_column_to_exist("percent_change_24h")
validator.expect_column_values_to_not_be_null("load_timestamp")

results = validator.validate(result_format="SUMMARY")

report_path = os.path.join(GX_REPORTS_DIR, "validation_report.json")
with open(report_path, "w") as f:
    json.dump(results.to_json_dict(), f, indent=2)

logging.info(f"Validation completed. Results saved -> {report_path}")

print("\n" + "="*80)
print("GREAT EXPECTATIONS VALIDATION SUMMARY")
print("="*80)
print(f"Total Expectations: {len(results.results)}")
print(f"Success: {results.success}")
print("="*80)
for res in results.results:
    name = res.expectation_config.type
    passed = "passed" if res.success else "failed"
    print(f"{passed} {name}")
print("="*80 + "\n")
