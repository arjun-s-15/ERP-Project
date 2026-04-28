import requests
from requests.auth import HTTPBasicAuth
import json
import os
from datetime import datetime, timezone
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from typing import Optional
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/pipelines", tags=["pipelines"])

# Configuration from environment variables
AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL")
AIRFLOW_USER = os.getenv("AIRFLOW_USER")
AIRFLOW_PASS = os.getenv("AIRFLOW_PASS")

class TriggerConfig(BaseModel):
    file_key: Optional[str] = "data/transformed_sample_dataset_6m.parquet"


def get_airflow_token():
    """Helper to authenticate and retrieve the JWT access token."""
    try:
        auth_response = requests.post(
            f"{AIRFLOW_BASE_URL}/auth/token",
            json={"username": AIRFLOW_USER, "password": AIRFLOW_PASS},
            headers={"Content-Type": "application/json"}
        )
        auth_response.raise_for_status()
        return auth_response.json()["access_token"]
    except Exception as e:
        print(f"Auth failed: {e}")
        raise HTTPException(status_code=500, detail="Failed to authenticate with Airflow")


def trigger_dag(dag_id: str, file_key: str):
    """Internal helper to execute the POST request to Airflow v2 API."""
    token = get_airflow_token()
    url = f"{AIRFLOW_BASE_URL}/api/v2/dags/{dag_id}/dagRuns"
    
    payload = {
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": {"file_key": file_key}
    }
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {token}"
    }

    response = requests.post(url, json=payload, headers=headers)
    
    print(f"DAG: {dag_id} | Status: {response.status_code}")
    print(f"Response Body: {response.json()}")
    
    return {
        "status_code": response.status_code,
        "airflow_response": response.json()
    }


@router.post("/trigger-master")
async def trigger_master_pipeline(config: TriggerConfig):
    """Triggers the Master Orchestrator for parallel transformations."""
    key = config.file_key or "data/transformed_sample_dataset_6m.parquet"
    return trigger_dag("master_sales_pipeline", key)


@router.post("/trigger-transform-sales")
async def trigger_transform_sales(config: TriggerConfig):
    """Triggers the Total Sales transformation DAG."""
    key = config.file_key or "data/transformed_sample_dataset_6m.parquet"
    return trigger_dag("sales_transform_dag", key)


@router.post("/trigger-transform-store")
async def trigger_transform_store(config: TriggerConfig):
    """Triggers the Store Sales transformation DAG."""
    key = config.file_key or "data/transformed_sample_dataset_6m.parquet"
    return trigger_dag("store_transform_dag", key)


@router.post("/trigger-forecast-total")
async def trigger_forecast_total(config: TriggerConfig):
    """Triggers the Forecast pipeline for Total Sales."""
    key = config.file_key or "data/daily_total_sales.parquet"
    return trigger_dag("sales_forecast_pipeline", key)


@router.post("/trigger-forecast-store")
async def trigger_forecast_store(config: TriggerConfig):
    """Triggers the Forecast pipeline for Store-level Sales."""
    key = config.file_key or "data/store_sales_forecast.parquet"
    return trigger_dag("store_forecast_pipeline", key)