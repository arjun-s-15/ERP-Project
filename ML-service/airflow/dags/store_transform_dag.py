import io
from datetime import datetime
import pandas as pd

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3Hook

from src.data_transformation import StoreSalesAnalyticsDataTransformation, StoreSalesForecastDataTransformation


BUCKET_NAME = "insighto-s3-bucket"
DEFAULT_FILE_KEY = "data/transformed_sample_dataset_6m.parquet"


def store_sales_transform(**context):
    """
    Directly triggered transformation for Store Sales.
    Handles both Forecast and Analytics outputs in one pass.
    """
    # 1. Retrieve the file key from the REST API 'conf' payload
    dag_run_conf = context.get("dag_run").conf or {}
    actual_key = dag_run_conf.get("file_key", DEFAULT_FILE_KEY)

    print(f"Executing Store Sales transformation for: {actual_key}")

    # 2. Read source parquet from S3
    s3 = S3Hook(aws_conn_id="aws_default")
    file_obj = s3.get_key(actual_key, bucket_name=BUCKET_NAME)

    if not file_obj:
        raise FileNotFoundError(f"Source file {actual_key} not found in {BUCKET_NAME}")
    
    body = file_obj.get()["Body"].read()
    buffer = io.BytesIO(body)
    del body 
    df = pd.read_parquet(buffer)
    del buffer
    df.columns = df.columns.str.strip()

    # 3. Define Transformations and Output Paths
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

    # 4. Apply and Upload Loop
    for item in transformations:
        # Instantiate and apply transformation
        transformer = item["class"](df.copy())
        transformed_df = transformer.apply_transformation()

        # Prepare buffer for S3 upload
        output_buffer = io.BytesIO()
        transformed_df.to_parquet(output_buffer, index=False)
        output_buffer.seek(0)

        # Upload transformed file to S3
        s3.get_conn().put_object(
            Bucket=BUCKET_NAME,
            Key=item["key"],
            Body=output_buffer.getvalue()
        )
        print(f"Uploaded: s3://{BUCKET_NAME}/{item['key']}")

    return "Successfully generated Forecast and Analytics files."




with DAG(
    dag_id="store_transform_dag",
    start_date=datetime(2024, 1, 1),
    tags=["elt", "store"],
) as dag:

    process_data = PythonOperator(
        task_id="store_sales_transform",
        python_callable=store_sales_transform
    )

    process_data