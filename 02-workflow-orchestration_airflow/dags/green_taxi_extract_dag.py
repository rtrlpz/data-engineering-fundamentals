from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
# from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import os
import requests
import gzip
import shutil


# === Configuration === #
OUTPUT_DIR = r"/opt/airflow/data/zipped_green_taxi_dataset"
if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

UNZIP_DIR =r'/opt/airflow/data/unzipped_raw_green_taxi_dataset'
if not os.path.exists(UNZIP_DIR):
    os.makedirs(UNZIP_DIR)

BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/"

# List of target files
FILES = [
    # 2019
    *[f"green_tripdata_2019-{i:02d}.csv.gz" for i in range(1, 13)],
    # 2020
    *[f"green_tripdata_2020-{i:02d}.csv.gz" for i in range(1, 13)],
    # 2021
    *[f"green_tripdata_2021-{i:02d}.csv.gz" for i in range(1, 8)],
]

def download_green_taxi_data():
    """Download and extract green_taxi_data CSV file."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for filename in FILES:
        url = BASE_URL + filename
        destination_path = os.path.join(OUTPUT_DIR, filename)

        if os.path.exists(destination_path):
            print(f"File already exists skipping: {filename}")
            continue

        print(f"Downloading: {filename}")
        response = requests.get(url, stream=True)
        response.raise_for_status()

        with open(destination_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)

        print(f"{filename} saved to {destination_path}")
    print("All files downloaded successfully.")


def unzip_green_taxi_data():
    os.makedirs(UNZIP_DIR, exist_ok=True)

    for filename in FILES:
        zipped_path = os.path.join(OUTPUT_DIR, filename)
        unzipped_path = os.path.join(UNZIP_DIR, filename.replace(".gz", ""))

        if os.path.exists(unzipped_path):
            print('File already exists skipping: {}'.format(filename))
            continue
        print(f"Unzipping: {zipped_path}")

        with gzip.open(zipped_path, "rb") as f_in:
            with open(unzipped_path, "wb") as f_out:
                shutil.copyfileobj(f_in, f_out)
        print(f"{filename} saved to {unzipped_path}")
    print("All files unzipped successfully.")


# === DAG Definition === #
default_args = {
    "owner": "airflow",
    "start_date": datetime(2019, 1, 1),
    "retries": 1
}

with DAG(
    dag_id="green_taxi_extract_dag",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags={"green_taxi", "extract", "taxi"},
    description="Extrac Green Taxi 2019-2021 data from DataTalksClub Github and store locally."
) as dag:
    extract_task = PythonOperator(
        task_id = 'extract_green_taxi_data',
        python_callable=download_green_taxi_data,
    )

    unzip_task = PythonOperator(
        task_id = 'unzip_green_taxi_data',
        python_callable=unzip_green_taxi_data,
    )

    extract_task >> unzip_task