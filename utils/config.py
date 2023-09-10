import os

# Kafka Config

KAFKA_SERVER = os.getenv("KAFKA_BOOTSTRAP")
CSV_KAFKA_TOPIC = "pipeline-csv"
ETL_KAFKA_TOPIC = "pipeline-etl"

base_path = os.getenv("BASE_PATH", os.path.expanduser("~"))

zip_dir = "zip"
csv_dir = "csv"
parquet_dir = "parquet"

zip_base_path = os.path.join(base_path, zip_dir)
csv_base_path = os.path.join(base_path, csv_dir)
parquet_base_path = os.path.join(base_path, parquet_dir)
