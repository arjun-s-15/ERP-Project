import json

from airflow.sdk import DAG, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3Hook, S3KeySensor
from airflow.providers.amazon.aws.sensors.sqs import SqsSensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

from datetime import datetime
import pandas as pd
import io

from src.utility import SQLQueryBuilder, SalesRawTableStrategy


BUCKET_NAME = "insighto-s3-bucket"
FILE_KEY = "data/transformed_sample_sales.csv"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/048013208170/InsightoQueue"

RAW_TABLE = "sales_raw"
TRANSFORMED_TABLE = "sales_transformed"


with DAG(
    dag_id="sales_elt_pipeline",
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
    # 2. Extract & Load
    # -----------------------------------------
    
    def extract_and_load(ti):
        sqs_data = ti.xcom_pull(key='messages', task_ids=['wait_for_sqs_msg'])
        print(f"DEBUG: Manually pulled XCom: {sqs_data}")
        if not sqs_data:
            raise ValueError("XCom is still None - checking Metadata DB...")

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
        
        s3 = S3Hook(aws_conn_id="aws_default")
        file_obj = s3.get_key(actual_key, bucket_name=BUCKET_NAME)
        
        # -----------------------------
        # Create table
        # -----------------------------
        pg_hook = PostgresHook(postgres_conn_id="app_postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        query_builder = SQLQueryBuilder(strategy=SalesRawTableStrategy(tablename=RAW_TABLE))
        query = query_builder.get_create_query()

        with engine.begin() as conn:
            conn.execute(text(query))

        for chunk in pd.read_csv(file_obj.get()["Body"], chunksize=25000):
            chunk.columns = chunk.columns.str.strip()
            
            chunk.to_sql(
                RAW_TABLE,
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

        return f"Successfully loaded {actual_key}"

    process_data = PythonOperator(
        task_id="extract_and_load",
        python_callable=extract_and_load
    )
    # -----------------------------------------
    # DAG Flow
    # -----------------------------------------
    wait_sensor >> process_data