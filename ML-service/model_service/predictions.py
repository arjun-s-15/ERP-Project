# analytics.py
from fastapi import APIRouter, HTTPException
import pandas as pd
import awswrangler as wr # Highly recommended for S3 + Parquet
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/predictions", tags=["predictions"])

STORE_FORECAST_PATH = "s3://insighto-s3-bucket/predictions/store_sales_predictions.csv"
TOTAL_FORECAST_PATH = "s3://insighto-s3-bucket/predictions/sales_predictions.csv"

def fetch_store_predictions():
    """Helper to read the CSV file from S3."""
    try:
        # pandas uses s3fs under the hood for s3:// paths
        # Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in env
        df = wr.s3.read_csv(path=STORE_FORECAST_PATH)
        df['feature_date'] = pd.to_datetime(df['feature_date'], format='mixed', errors='coerce')
        df['prediction_date'] = pd.to_datetime(df['prediction_date'], format='mixed', errors='coerce')
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 Read Error: {str(e)}")
    
def fetch_total_predictions():
    """Helper to read the CSV file from S3."""
    try:
        # pandas uses s3fs under the hood for s3:// paths
        # Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in env
        df = wr.s3.read_csv(path=TOTAL_FORECAST_PATH)
        df['feature_date'] = pd.to_datetime(df['feature_date'], format='mixed', errors='coerce')
        df['prediction_date'] = pd.to_datetime(df['prediction_date'], format='mixed', errors='coerce')
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 Read Error: {str(e)}")

@router.get("/sales-forecast/store")
async def get_sales_forecast():
    """Returns the sales forecast for the next day"""
    df = fetch_store_predictions()
    
    # Extracting the relevant columns for the forecast
    # forecast_data = df[['location_id', 'datetime', 'predicted_quantity']]
    forecast_data = df.sort_values("prediction_date")
    
    # Convert to dictionary for JSON response
    return forecast_data.to_dict(orient="records")

@router.get("/sales-forecast/total")
async def get_total_sales_forecast():
    """Returns the total sales forecast for the next day"""
    df = fetch_total_predictions()
    
    # Extracting the relevant columns for the forecast
    forecast_data = df.sort_values("prediction_date")
    
    # Convert to dictionary for JSON response
    return forecast_data.to_dict(orient="records")