from datetime import datetime, timedelta, timezone
import io
import json
from airflow import DAG
from airflow.sdk import dag, task 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from dotenv import load_dotenv
import pandas as pd
import requests
load_dotenv()

DEFAULT_FILE_KEY = "data/store_sales_forecast.parquet"
BUCKET_NAME = "insighto-s3-bucket"
API_URL = "http://model-service:8000/predict_store_sales"

default_args = {
    'owner': 'atharv',
    'retries': 1
}

def predict_tomorrow(**context):
    """
    Directly triggered forecast for all stores found in the dataset.
    """
    # 1. Get file_key from REST API params or default
    dag_run_conf = context.get("dag_run").conf or {}
    actual_key = dag_run_conf.get("file_key", DEFAULT_FILE_KEY)

    print(f"Executing Store Forecast for file: {actual_key}")

    # 2. Read parquet from S3
    s3 = S3Hook(aws_conn_id="aws_default")
    file_obj = s3.get_key(actual_key, bucket_name=BUCKET_NAME)

    if not file_obj:
        raise FileNotFoundError(f"S3 file not found: {actual_key}")
    
    body = file_obj.get()["Body"].read()
    buffer = io.BytesIO(body)
    del body
    df = pd.read_parquet(buffer)
    del buffer
    df.columns = df.columns.str.strip()

    # Pre-processing
    df = df.rename(columns={"quantity": "total_sales"})
    df['datetime'] = pd.to_datetime(df['datetime'])

    all_store_predictions = {}
    locations = df['location_id'].unique()

    print(f"Found {len(locations)} locations. Starting sequential predictions...")

    for loc_id in locations:
        loc_id_str = str(loc_id)
        # Isolate specific store data and get the most recent row
        store_subset = df[df['location_id'] == loc_id].sort_values("datetime")
        latest_entry = store_subset.iloc[[-1]]

        print(f"Predicting Store {loc_id_str} (Last Date: {latest_entry['datetime'].iloc[0]})")

        # Prepare payload (Drop location_id from features as per model service requirement)
        X_inference = latest_entry.drop(columns=['location_id'])

        try:
            payload = {
                "location_id": loc_id_str,
                "input_data": X_inference.to_json()
            }
            response = requests.post(API_URL, json=payload)
            response.raise_for_status()

            all_store_predictions[loc_id_str] = response.json()
        except Exception as e:
            print(f"Failed prediction for Store {loc_id_str}: {e}")
            all_store_predictions[loc_id_str] = {"error": str(e)}

    # Push all results to XCom
    context['ti'].xcom_push(key="prediction_response", value=all_store_predictions)
    print("Sequential store predictions completed.")


def store_predictions(**context):
    """
    Collects predictions for all stores and appends to the central CSV database.
    """
    ti = context['ti']
    all_data = ti.xcom_pull(key="prediction_response", task_ids=["predict_tomorrow"])[0]
    if not all_data:
        raise ValueError("No prediction data retrieved from XCom.")
    
    new_records = []

    for loc_id, pred_details in all_data.items():
        if "error" in pred_details:
            print(f"Skipping Store {loc_id} due to error: {pred_details['error']}")
            continue

        try:
            feature_date = datetime.strptime(pred_details["datetime"], "%Y-%m-%d %H:%M:%S")
            new_records.append({
                "location_id": loc_id,
                "feature_date": feature_date,
                "prediction_date": feature_date + timedelta(days=1),
                "predicted_sales": pred_details["prediction"],
                "model_name": pred_details["model_name"],
                "model_version": pred_details["model_version"]
            })
        except Exception as e:
            print(f"Error parsing record for Store {loc_id}: {e}")

    if not new_records:
        print("No valid prediction records to store.")
        return
    
    output_key = "predictions/store_sales_predictions.csv"
    s3 = S3Hook(aws_conn_id="aws_default")

    # Handle existing CSV append
    try:
        existing_obj = s3.get_key(output_key, bucket_name=BUCKET_NAME)
        existing_df = pd.read_csv(io.BytesIO(existing_obj.get()["Body"].read()))
    except Exception:
        existing_df = pd.DataFrame()  # No existing file, start fresh

    combined_df = pd.concat([existing_df, pd.DataFrame(new_records)], ignore_index=True)

    buffer = io.BytesIO()
    combined_df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3.get_conn().put_object(
        Bucket=BUCKET_NAME,
        Key=output_key,
        Body=buffer.getvalue()
    )
    print(f"Updated store predictions file. New total: {len(combined_df)} records.")


with DAG(
    dag_id="store_forecast_pipeline",
    start_date=datetime(2026, 3, 25),
) as dag:

    predict_sales = PythonOperator(
        task_id="predict_tomorrow",
        python_callable=predict_tomorrow
    )

    upload_results = PythonOperator(
        task_id="store_predictions",
        python_callable=store_predictions
    )
    
    predict_sales >> upload_results
