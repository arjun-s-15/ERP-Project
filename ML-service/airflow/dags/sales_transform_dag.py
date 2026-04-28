import json

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor

from datetime import datetime
import pandas as pd
import io

from src.data_transformation import DailySalesDataTransformation


BUCKET_NAME = "insighto-s3-bucket"
DEFAULT_FILE_KEY = "data/transformed_sample_dataset_6m.parquet"


def sales_transform(**context):
    """
    Logic to transform sales data.
    """
    # 1. Retrieve the file key from the REST API 'conf' payload
    # If triggered via API: {"conf": {"file_key": "your/path/here.parquet"}}
    dag_run_conf = context.get("dag_run").conf or {}
    actual_key = dag_run_conf.get("file_key", DEFAULT_FILE_KEY)

    print(f"Executing transformation for file: {actual_key}")

    # 2. Read parquet from S3
    s3 = S3Hook(aws_conn_id="aws_default")

    file_obj = s3.get_key(actual_key, bucket_name=BUCKET_NAME)
    if not file_obj:
        raise FileNotFoundError(f"File {actual_key} not found in bucket {BUCKET_NAME}")
    
    body = file_obj.get()["Body"].read()
    buffer = io.BytesIO(body)
    del body 
    df = pd.read_parquet(buffer)
    del buffer
    df.columns = df.columns.str.strip()

    # 3. Apply transformation for Daily Sales
    transformer = DailySalesDataTransformation(df)
    transformed_df = transformer.apply_transformation()

    # 4. Upload result back to S3
    output_key = "data/daily_total_sales.parquet"
    output_buffer = io.BytesIO() 
    transformed_df.to_parquet(output_buffer, index=False)
    output_buffer.seek(0)

    s3.get_conn().put_object(
            Bucket=BUCKET_NAME,
            Key=output_key,
            Body=output_buffer.getvalue()
        )
    print(f"Uploaded: s3://{BUCKET_NAME}/{output_key}")
    return f"Processed {actual_key} and uploaded to {output_key}"




with DAG(
    dag_id="sales_transform_dag",
    start_date=datetime(2024, 1, 1),
    tags=["elt", "sales"],
) as dag:

    process_data = PythonOperator(
        task_id="sales_transform",
        python_callable=sales_transform,
        # provide_context is True by default in Airflow 2.x, 
        # but explicit is better for clarity.
    )
    # -----------------------------------------
    # DAG Flow
    # -----------------------------------------
    process_data