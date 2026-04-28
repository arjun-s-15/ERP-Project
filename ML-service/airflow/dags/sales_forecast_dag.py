from datetime import datetime, timedelta, timezone
import io
import json
from airflow import DAG
from airflow.sdk import dag, task 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from dotenv import load_dotenv
import pandas as pd
import requests
load_dotenv()

DEFAULT_FILE_KEY = "data/daily_total_sales.parquet"
BUCKET_NAME = "insighto-s3-bucket"
API_URL = "http://model_service:8000/predict_sales"

default_args = {
    'owner': 'atharv',
    'retries': 1
}

def predict_tomorrow(**context):
        """
        Fetches the latest processed data and requests a forecast from the model service.

        Args:
            None

        Returns:
            dict: The JSON response from the model service containing predictions 
                and metadata.
        """
        # 1. Get file_key from trigger config or use default
        dag_run_conf = context.get("dag_run").conf or {}
        actual_key = dag_run_conf.get("file_key", DEFAULT_FILE_KEY)

        print(f"Executing forecast using file: {actual_key}")

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

        # Prepare input data (last record)
        input_df = df.set_index("datetime").sort_index(ascending=True)
        input_data = input_df.iloc[[-1]]
        print("Input data retrieved: ", input_data)

        # 3. Request model service
        try:
            payload = {
                "input_data": input_data.reset_index().to_json()
            }
            response = requests.post(API_URL, json=payload)
            response.raise_for_status()
            data = response.json()
            
            print("Got the model service response: ")
            print(data)
            context['ti'].xcom_push(key="prediction_response", value=data)
        except Exception as e:
            print(f"Request to model service failed: {e}")
            raise


def store_predictions(**context):
    """
    Parses model inference results and stores them in the predictions table.

        Args:
            data (dict): The dictionary response from the model service containing 
                        prediction values and metadata.

        Returns:
            None
    """
    ti = context['ti']
    data = ti.xcom_pull(key="prediction_response", task_ids=["predict_tomorrow"])[0]

    if not data:
        raise ValueError("No prediction data found in XCom.")
    
    feature_date = datetime.strptime(data["datetime"], "%Y-%m-%d %H:%M:%S")
    record = {
        "feature_date": feature_date,
        "prediction_date": feature_date + timedelta(days=1),
        "predicted_sales": data["prediction"],
        "model_name": data["model_name"],
        "model_version": data["model_version"]
    }

    output_key = "predictions/sales_predictions.csv"
    s3 = S3Hook(aws_conn_id="aws_default")

    # Load existing or create new
    try:
        existing_obj = s3.get_key(output_key, bucket_name=BUCKET_NAME)
        existing_df = pd.read_csv(io.BytesIO(existing_obj.get()["Body"].read()))
    except Exception:
        existing_df = pd.DataFrame()  # file doesn't exist yet — start fresh

    new_df = pd.DataFrame([record])
    combined_df = pd.concat([existing_df, new_df], ignore_index=True)

    # Upload back to S3
    buffer = io.BytesIO()
    combined_df.to_csv(buffer, index=False)
    buffer.seek(0)

    s3.get_conn().put_object(
        Bucket=BUCKET_NAME,
        Key=output_key,
        Body=buffer.getvalue()
    )
    print(f"Total records: {len(combined_df)} added to S3")


with DAG(
    dag_id="sales_forecast_pipeline",
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
