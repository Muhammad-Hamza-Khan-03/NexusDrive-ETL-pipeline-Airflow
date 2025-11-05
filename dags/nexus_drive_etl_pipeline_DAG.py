from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import TaskGroup

import pandas as pd
import tempfile
import os
import logging
import sys

# Add DAGs folder to Python path for custom module imports
DAGS_FOLDER = os.path.dirname(os.path.abspath(__file__))
if DAGS_FOLDER not in sys.path:
    sys.path.insert(0, DAGS_FOLDER)

# Custom modules
from data_ingest import DataIngest
from data_transformation import DataAligner

DEBUG_MODE = True   # toggle to False for full pipeline
MAX_ROWS = 500      # number of rows to load per CSV

# ------------------ Default Args ------------------ #
default_args = {
    "owner": "etl",
    "start_date": datetime(2025, 11, 1),
    "retries": 1,
    "depends_on_past": False,
}

# ------------------ DAG Definition ------------------ #
dag = DAG(
    dag_id="etl_delivery_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    max_active_tasks=1,
)

# ------------------ Configuration ------------------ #
BUCKET_NAME = "pickup-delivery"
AWS_CONN_ID = "minio_default"
CITIES = ["jl","yt","hz","cq","sh"]
AMAZON_KEY = "amazon_delivery.csv"

logger = logging.getLogger(__name__)

# ------------------ Helper Functions ------------------ #

def enrich_city_data(city, **kwargs):
    """Enrich delivery data for one city using weather, traffic, and vehicle info."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            delivery_path = os.path.join(tmp_dir, f"delivery_{city}.csv")
            weather_path = os.path.join(tmp_dir, f"{city}_weather.csv")
            enriched_path = os.path.join(tmp_dir, f"enriched_{city}.csv")

            logger.info(f"Downloading delivery file for {city}")
            delivery_obj = s3_hook.get_key(
                key=f"Pickup_and_delivery_data/delivery/delivery_{city}.csv",
                bucket_name=BUCKET_NAME
            )
            with open(delivery_path, 'wb') as f:
                f.write(delivery_obj.get()['Body'].read())
            
            logger.info(f"Downloading weather file for {city}")
            weather_obj = s3_hook.get_key(
                key=f"Pickup_and_delivery_data/weather/{city}_weather.csv",
                bucket_name=BUCKET_NAME
            )
            with open(weather_path, 'wb') as f:
                f.write(weather_obj.get()['Body'].read())

            logger.info(f"Starting enrichment for {city}")
            loader = DataIngest(delivery_path, weather_path)
            enriched_df = loader.enrich_with_weather()
            enriched_df = loader.enrich_with_traffic_and_vehicles()

            # ✅ Add city and calculate ETA
            enriched_df["city"] = city
            
            # ✅ accept_time already exists, just ensure it's datetime
            enriched_df["accept_time"] = pd.to_datetime(
                enriched_df["accept_time"], 
                errors="coerce"
            )
            
            # ✅ delivery_time already exists, just ensure it's datetime
            enriched_df["delivery_time"] = pd.to_datetime(
                enriched_df["delivery_time"], 
                errors="coerce"
            )
            
            # ✅ Calculate ETA in minutes
            enriched_df["ETA_target"] = (
                (enriched_df["delivery_time"] - enriched_df["accept_time"]).dt.total_seconds() / 60
            )

            # Save and upload
            enriched_df.to_csv(enriched_path, index=False)
            logger.info(f"Saved enriched file temporarily at {enriched_path}")
            
            s3_hook.load_file(
                filename=enriched_path,
                key=f"Pickup_and_delivery_data/Enriched/enriched_{city}.csv",
                bucket_name=BUCKET_NAME,
                replace=True,
            )

            logger.info(f"✅ Enriched and uploaded data for {city}. Shape: {enriched_df.shape}")
            
        except Exception as e:
            logger.error(f"❌ Failed to process city {city}: {str(e)}")
            raise
def combine_enriched_datasets(**kwargs):
    """Combine enriched datasets from all cities."""
    s3_hook = S3Hook(aws_conn_id=AWS_CONN_ID)
    
    with tempfile.TemporaryDirectory() as tmp_dir:
        try:
            combined_path = os.path.join(tmp_dir, "combined_enriched.csv")
            
            dfs = []
            for city in CITIES:
                local_path = os.path.join(tmp_dir, f"enriched_{city}.csv")
                
                logger.info(f"Downloading enriched file for {city}")
                
                # ✅ Use get_key method
                enriched_obj = s3_hook.get_key(
                    key=f"Pickup_and_delivery_data/Enriched/enriched_{city}.csv",
                    bucket_name=BUCKET_NAME
                )
                with open(local_path, 'wb') as f:
                    f.write(enriched_obj.get()['Body'].read())
                
                dfs.append(pd.read_csv(local_path, nrows=MAX_ROWS if DEBUG_MODE else None))

            # ✅ Combine all dataframes
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df.to_csv(combined_path, index=False)

            # ✅ Upload combined file to MinIO
            s3_hook.load_file(
                filename=combined_path,
                key="Pickup_and_delivery_data/Final/combined_enriched.csv",
                bucket_name=BUCKET_NAME,
                replace=True,
            )

            logger.info(f"✅ Combined enriched dataset uploaded. Shape: {combined_df.shape}")
            
            # ✅ Pass the S3 key via XCom
            kwargs["ti"].xcom_push(key="combined_enriched_key", value="Pickup_and_delivery_data/Final/combined_enriched.csv")
            
        except Exception as e:
            logger.error(f"❌ Failed to combine datasets: {str(e)}")
            raise

def push_data():
    print("Data Pushed to Minio Successfully")

# ------------------ Tasks ------------------ #
with dag:
    with TaskGroup("process_cities") as process_cities:
        for city in CITIES:
            PythonOperator(
                task_id=f"Extract_data_{city}",
                python_callable=enrich_city_data,
                op_kwargs={"city": city},
            )

    combine_task = PythonOperator(
        task_id="Join_and_Transform_data",
        python_callable=combine_enriched_datasets,
    )
    push_data_task = PythonOperator(
        task_id="Load_Data",
        python_callable=push_data,
    )
    # DAG flow
    process_cities >> combine_task >>push_data_task
