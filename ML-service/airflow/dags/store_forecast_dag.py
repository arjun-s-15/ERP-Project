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

FILE_KEY = "data/store_sales_forecast.parquet"
BUCKET_NAME = "insighto-s3-bucket"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/048013208170/InsightoQueue"
API_URL = "http://model_service:8000/predict_store_sales"

default_args = {
    'owner': 'atharv',
    'retries': 1
}

with DAG(
    dag_id="store_forecast_pipeline",
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
        body = file_obj.get()["Body"].read()
        buffer = io.BytesIO(body)  
        del body
        df = pd.read_parquet(buffer)
        del buffer
        df.columns = df.columns.str.strip()

        df = df.rename(columns={"quantity": "total_sales"})
        df['datetime'] = pd.to_datetime(df['datetime'])

        all_store_predictions = {}
        locations = df['location_id'].unique()

        print(f"Found {len(locations)} locations. Starting sequential predictions...")

        for loc_id in locations:
            # Isolate store data and get the absolute last row (most recent)
            store_subset = df[df['location_id'] == loc_id].sort_values("datetime")
            latest_entry = store_subset.iloc[[-1]]

            loc_id_str = str(loc_id)
            print(f"Predicting for Store {loc_id_str} (Latest Date: {latest_entry['datetime'].iloc[0]})")

            # We keep loc_id_str for the payload metadata but remove it from the dataframe
            X_inference = latest_entry.drop(columns=['location_id'])

            # 4. Request prediction from the new store-specific endpoint
            # Note: Using '/predict_store_sales' as defined in your FastAPI service
            try:
                payload = {
                    "location_id": loc_id_str,
                    "input_data": X_inference.to_json()
                }

                # API_URL should now point to: http://<host>:8000/predict_store_sales
                response = requests.post(API_URL, json=payload)
                response.raise_for_status()

                prediction_data = response.json()
                all_store_predictions[loc_id_str] = prediction_data

            except Exception as e:
                print(f"Failed prediction for Store {loc_id_str}: {e}")
                all_store_predictions[loc_id_str] = {"error": str(e)}

            ti.xcom_push(key="prediction_response", value=all_store_predictions)
            print("All store predictions completed.")
        
    

    def store_predictions(ti):
        """
        Parses model inference results and stores them in the predictions table.

        Args:
            data (dict): The dictionary response from the model service containing 
                        prediction values and metadata.

        Returns:
            None
        """
        all_data = ti.xcom_pull(key="prediction_response", task_ids=["predict_tomorrow"])[0]
        if not all_data:
            print("No prediction data found in XCom.")
            return
        
        new_records = []

        # Iterate through each store's prediction result
        for loc_id, pred_details in all_data.items():
            if "error" in pred_details:
                print(f"Skipping storage for Store {loc_id} due to previous error: {pred_details['error']}")
                continue

            try:
                # Parse the datetime from the API response
                feature_date = datetime.strptime(pred_details["datetime"], "%Y-%m-%d %H:%M:%S")
                
                new_records.append({
                    "location_id": loc_id, # Essential for multi-store tracking
                    "feature_date": feature_date,
                    "prediction_date": feature_date + timedelta(days=1),
                    "predicted_sales": pred_details["prediction"],
                    "model_name": pred_details["model_name"],
                    "model_version": pred_details["model_version"]
                })
            except Exception as e:
                print(f"Error parsing record for Store {loc_id}: {e}")
            
        if not new_records:
            print("No valid records to store.")
            return
        
        # 3. Manage the S3 "Database" (CSV file)
        output_key = "predictions/store_sales_predictions.csv"
        s3 = S3Hook(aws_conn_id="aws_default")
        try:
            existing_obj = s3.get_key(output_key, bucket_name=BUCKET_NAME)
            existing_df = pd.read_csv(io.BytesIO(existing_obj.get()["Body"].read()))
        except Exception:
            existing_df = pd.DataFrame()  # file doesn't exist yet — start fresh
        
        # 4. Append and Upload
        new_df = pd.DataFrame(new_records)
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        buffer = io.BytesIO()
        combined_df.to_csv(buffer, index=False)
        buffer.seek(0)

        s3.get_conn().put_object(
            Bucket=BUCKET_NAME,
            Key=output_key,
            Body=buffer.getvalue()
        )
        print(f"Appended {len(new_records)} store records. Total file records: {len(combined_df)}")


    predict_sales = PythonOperator(
        task_id="predict_tomorrow",
        python_callable=predict_tomorrow
    )

    upload_results = PythonOperator(
        task_id="store_predictions",
        python_callable=store_predictions
    )
    
    wait_sensor >> predict_sales >> upload_results
