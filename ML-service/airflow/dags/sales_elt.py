from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text

from datetime import datetime
import pandas as pd
import io

from src.utility import SQLQueryBuilder, SalesRawTableStrategy


BUCKET_NAME = "insighto-s3-bucket"
FILE_KEY = "transformed_sample_sales.csv"

RAW_TABLE = "sales_raw"
TRANSFORMED_TABLE = "sales_transformed"


with DAG(
    dag_id="sales_elt_pipeline",
    start_date=datetime(2024, 1, 1),
    tags=["elt", "sales"],
) as dag:

    # -----------------------------------------
    # 1. S3 Sensor (trigger on file update)
    # -----------------------------------------
    wait_for_s3_file = S3KeySensor(
        task_id="wait_for_s3_file",
        bucket_name=BUCKET_NAME,
        bucket_key=FILE_KEY,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=60 * 60,
        mode="poke"
    )

    # -----------------------------------------
    # 2. Extract & Load
    # -----------------------------------------
    @task
    def extract_and_load():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        s3 = S3Hook(aws_conn_id="aws_default")
        file_obj = s3.get_key(FILE_KEY, bucket_name=BUCKET_NAME)
        
        # -----------------------------
        # Create table
        # -----------------------------
        pg_hook = PostgresHook(postgres_conn_id="app_postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        query_builder = SQLQueryBuilder(strategy=SalesRawTableStrategy(tablename=RAW_TABLE))
        query = query_builder.get_create_query()

        with engine.begin() as conn:
            conn.execute(text(query))

        for chunk in pd.read_csv(file_obj.get()["Body"], chunksize=50000):
            chunk.columns = chunk.columns.str.strip()

            chunk.to_sql(
                RAW_TABLE,
                engine,
                if_exists="append",
                index=False,
                method="multi"
            )

        return "Data loaded successfully"

    # -----------------------------------------
    # 3. Transform (ELT step)
    # -----------------------------------------
    @task
    def transform_data():
        pg_hook = PostgresHook(postgres_conn_id="app_postgres")
        engine = pg_hook.get_sqlalchemy_engine()

        # Read from raw table
        df = pd.read_sql(f"SELECT * FROM {RAW_TABLE}", engine)

        # -----------------------------
        # Transformations
        # -----------------------------
        df.columns = df.columns.str.strip().str.lower()

        # Example transformations (customize)
        if "quantity" in df.columns:
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

        if "price" in df.columns:
            df["price"] = pd.to_numeric(df["price"], errors="coerce")

        if "invoicedate" in df.columns:
            df["invoicedate"] = pd.to_datetime(df["invoicedate"], errors="coerce")

        # Create revenue column
        if "quantity" in df.columns and "price" in df.columns:
            df["revenue"] = df["quantity"] * df["price"]

        # Drop null timestamps (example rule)
        if "invoicedate" in df.columns:
            df = df.dropna(subset=["invoicedate"])

        # -----------------------------
        # Load transformed table
        # -----------------------------
        df.to_sql(
            TRANSFORMED_TABLE,
            engine,
            if_exists="replace",
            index=False
        )

        return f"Transformed data stored in {TRANSFORMED_TABLE}"

    # -----------------------------------------
    # DAG Flow
    # -----------------------------------------
    wait_for_s3_file >> extract_and_load()