from datetime import datetime, timedelta, timezone
import io
import json
from airflow import DAG
from airflow.sdk import dag, task 
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from dotenv import load_dotenv
# from src.utility import next_forex_trading_day, SQLTableBuilder, PredictionTableStrategy  #type: ignore
import pandas as pd
import requests
load_dotenv()

FILE_KEY = "data/daily_total_sales.parquet"
BUCKET_NAME = "insighto-s3-bucket"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/048013208170/InsightoQueue"
API_URL = "http://model-service:8000/predict_sales"

default_args = {
    'owner': 'atharv',
    'retries': 1
}

with DAG(
    dag_id="sales_forecast_pipeline",
    start_date=datetime(2026, 3, 25),
) as dag:
    # -----------------------------------------
    # 1. SQS Sensor (listen for any S3 event in the queue)
    # -----------------------------------------
    wait_sensor = SqsSensor(
        task_id="wait_for_sqs_msg",
        sqs_queue=SQS_QUEUE_URL,
        aws_conn_id="aws_default",
        max_messages=1,
        wait_time_seconds=20,
        mode="reschedule",
        poke_interval=60,
        do_xcom_push=True
    )

    # -----------------------------------------
    # 2. Predict tomorrow's sales
    # -----------------------------------------
    def predict_tomorrow(ti):
        """
        Fetches the latest processed data and requests a forecast from the model service.

        Args:
            None

        Returns:
            dict: The JSON response from the model service containing predictions 
                and metadata.
        """
        # Pull SQS message from XCom
        sqs_data = ti.xcom_pull(key='messages', task_ids=['wait_for_sqs_msg'])
        print(f"DEBUG: Manually pulled XCom: {sqs_data}")
        if not sqs_data:
            raise ValueError("XCom is still None - checking Metadata DB...")

        # Parse S3 event metadata from SQS body
        message_content = json.loads(sqs_data[0][0]['Body'])

        try:
            record = message_content['Records'][0]
            actual_key = record['s3']['object']['key']
            event_time = record['eventTime']
            print(f"Event detected at {event_time}. File: {actual_key}")
        except (KeyError, IndexError) as e:
            print(f"Error parsing SQS message: {e}")
            return

        if actual_key != FILE_KEY:
            print(f"Skipping: {actual_key} does not match {FILE_KEY}")
            return f"Skipped {actual_key}"

        # Read parquet from S3
        s3 = S3Hook(aws_conn_id="aws_default")

        file_obj = s3.get_key(actual_key, bucket_name=BUCKET_NAME)
        # file_obj = s3.get_key(FILE_KEY, bucket_name=BUCKET_NAME)
        body = file_obj.get()["Body"].read()
        buffer = io.BytesIO(body)  
        del body
        df = pd.read_parquet(buffer)
        del buffer
        df.columns = df.columns.str.strip()
        input_df = df.set_index("datetime").sort_index(ascending=True)
        input_data = input_df.iloc[[-1]]
        print("Input data retrieved: ", input_data)

        try:
            payload = {
                "input_data": input_data.reset_index().to_json()
            }
            response = requests.post(API_URL, json=payload)
            data = response.json()
            print("Got the model service response: ")
            print(data)
            ti.xcom_push(key="prediction_response", value=data)
        except Exception as e:
            print(f"Request to model service failed: {e}")
    

    def store_predictions(ti):
        """
        Parses model inference results and stores them in the predictions table.

        Args:
            data (dict): The dictionary response from the model service containing 
                        prediction values and metadata.

        Returns:
            None
        """
        data = ti.xcom_pull(key="prediction_response", task_ids=["predict_tomorrow"])[0]
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
        try:
            existing_obj = s3.get_key(output_key, bucket_name=BUCKET_NAME)
            existing_df = pd.read_csv(io.BytesIO(existing_obj.get()["Body"].read()))
        except Exception:
            existing_df = pd.DataFrame()  # file doesn't exist yet — start fresh
        
        new_df = pd.DataFrame([record])
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        buffer = io.BytesIO()
        combined_df.to_csv(buffer, index=False)
        buffer.seek(0)

        s3.get_conn().put_object(
            Bucket=BUCKET_NAME,
            Key=output_key,
            Body=buffer.getvalue()
        )
        print(f"Total records: {len(combined_df)} added to S3")
        print("Record(s) added to the database.")


    predict_sales = PythonOperator(
        task_id="predict_tomorrow",
        python_callable=predict_tomorrow
    )

    upload_results = PythonOperator(
        task_id="store_predictions",
        python_callable=store_predictions
    )
    
    wait_sensor >> predict_sales >> upload_results
