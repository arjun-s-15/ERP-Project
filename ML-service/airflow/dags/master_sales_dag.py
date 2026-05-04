from airflow import DAG 
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime 

with DAG(
    dag_id="master_sales_pipeline",
    start_date=datetime(2024, 1, 1),
    tags=["master", "sales"]
) as dag:
    # Total sales branch
    transform_total = TriggerDagRunOperator(
        task_id="trigger_sales_transform_dag",
        trigger_dag_id="sales_transform_dag",
        conf={"file_key": "{{dag_run.conf['file_key']}}"},
        wait_for_completion=True,
    )

    train_total = TriggerDagRunOperator(
        task_id="trigger_sales_train_dag",
        trigger_dag_id="sales_train_pipeline",
        conf={},
        wait_for_completion=True,
    )

    predict_total = TriggerDagRunOperator(
        task_id="trigger_sales_forecast_dag",
        trigger_dag_id="sales_forecast_pipeline",
        conf={}
    )

    # Store sales branch
    transform_store = TriggerDagRunOperator(
        task_id="trigger_store_transform_dag",
        trigger_dag_id="store_transform_dag",
        conf={"file_key": "{{dag_run.conf['file_key']}}"},
        wait_for_completion=True,
    )

    train_store = TriggerDagRunOperator(
        task_id="trigger_store_train_dag",
        trigger_dag_id="store_train_pipeline",
        conf={},
        wait_for_completion=True,
    )

    predict_store = TriggerDagRunOperator(
        task_id="trigger_store_forecast_dag",
        trigger_dag_id="store_forecast_pipeline",
        conf={}
    )

    transform_total >> train_total >> predict_total
    transform_store >> train_store >> predict_store