import json

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

from datetime import datetime
import pandas as pd
import io

from src.data_transformation import StoreSalesAnalyticsDataTransformation, StoreSalesForecastDataTransformation


BUCKET_NAME = "insighto-s3-bucket"
FILE_KEY = "data/transformed_sample_dataset_6m.parquet"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/048013208170/InsightoQueue"

RAW_TABLE = "sales_raw"
TRANSFORMED_TABLE = "store_transformed"


with DAG(
    dag_id="store_transform_dag",
    start_date=datetime(2024, 1, 1),
    tags=["elt", "sales"],
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
    # 2. Transform
    # -----------------------------------------
    
    def sales_transform(ti):
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

        # 2. Define Transformations and Output Paths
        transformations = [
            {
                "class": StoreSalesForecastDataTransformation,
                "key": "data/store_sales_forecast.parquet"
            },
            {
                "class": StoreSalesAnalyticsDataTransformation,
                "key": "data/store_sales_analytics.parquet"
            }
        ]

        # 3. Apply and Upload
        for item in transformations:
            # Instantiate and apply
            transformer = item["class"](df.copy())
            transformed_df = transformer.apply_transformation()

            # Upload to S3
            output_buffer = io.BytesIO()
            transformed_df.to_parquet(output_buffer, index=False)
            output_buffer.seek(0)

            s3.get_conn().put_object(
                Bucket=BUCKET_NAME,
                Key=item["key"],
                Body=output_buffer.getvalue()
            )
            print(f"Uploaded: s3://{BUCKET_NAME}/{item['key']}")
        
        return "Successfully generated Forecast and Analytics files."

    process_data = PythonOperator(
        task_id="sales_transform",
        python_callable=sales_transform
    )
    # -----------------------------------------
    # DAG Flow
    # -----------------------------------------
    wait_sensor >> process_data