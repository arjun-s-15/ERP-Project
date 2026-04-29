# analytics.py
from fastapi import APIRouter, HTTPException
import pandas as pd
import awswrangler as wr # Highly recommended for S3 + Parquet
from dotenv import load_dotenv

load_dotenv()

router = APIRouter(prefix="/analytics", tags=["analytics"])

BUCKET_NAME = "insighto-s3-bucket"

S3_PATH = "s3://insighto-s3-bucket/data/store_sales_analytics.parquet"

def fetch_analytics_data(key: str):
    """Helper to read the parquet file from S3."""
    try:
        # pandas uses s3fs under the hood for s3:// paths
        # Ensure AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY are in env
        path = f"s3://{BUCKET_NAME}/data/{key}"
        df = wr.s3.read_parquet(path=path)
        df['datetime'] = pd.to_datetime(df['datetime'])
        return df
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"S3 Read Error: {str(e)}")

@router.get("/total-store-sales")
async def get_total_sales_per_store():
    """1. Returns the aggregate total sales for each store location."""
    df = fetch_analytics_data("store_sales_analytics.parquet")
    
    # Grouping by location and summing quantity
    total_sales = df.groupby('location_id')['quantity'].sum().reset_index()
    
    # Convert to dictionary for JSON response
    return total_sales.to_dict(orient="records")


@router.get("/monthly-store-sales")
async def get_monthly_sales_per_store():
    """2. Returns monthly sales breakdown for each store."""
    df = fetch_analytics_data("store_sales_analytics.parquet")
    
    # Extract month and year for grouping
    df['month_year'] = df['datetime'].dt.to_period('M').astype(str)
    
    # Group by location and the monthly period
    monthly_stats = df.groupby(['location_id', 'month_year'])['quantity'].sum().reset_index()
    
    # Pivot or structure for better frontend/analytics consumption
    # Structure: [{'location_id': 1, 'month_year': '2017-01', 'quantity': 1234.5}, ...]
    return monthly_stats.to_dict(orient="records")


@router.get("/day-of-week-sales")
async def get_day_of_week_sales():
    """3. Returns total sales for every day of the week per store."""
    df = fetch_analytics_data("store_sales_analytics.parquet")
    
    # Optional: Map numeric days to names for clearer analytics
    day_map = {
        0: "Monday", 1: "Tuesday", 2: "Wednesday", 
        3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"
    }
    
    # Grouping by store and day of week
    dow_stats = df.groupby(['location_id', 'day_of_week'])['quantity'].sum().reset_index()
    
    # Apply mapping
    dow_stats['day_name'] = dow_stats['day_of_week'].map(day_map)
    
    # Sort for logical progression (Mon -> Sun)
    dow_stats = dow_stats.sort_values(['location_id', 'day_of_week'])
    
    return dow_stats.to_dict(orient="records")


@router.get("/historical/total-daily-sales")
async def get_total_daily_sales():
    """4. Returns total sales for each day."""
    df = fetch_analytics_data("daily_total_sales.parquet")
    
    try:
        df = df[["datetime", "total_sales"]]
        df = df.sort_values("datetime")  # Ensure chronological order
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d")  # Format for JSON serialization
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data Processing Error: {str(e)}")
    

@router.get("/historical/store_daily_sales")
async def get_store_daily_sales():
    """5. Returns daily sales for each store."""
    df = fetch_analytics_data("store_sales_analytics.parquet")
    
    try:
        df = df[["datetime", "location_id", "quantity"]]
        df = df.sort_values(["location_id", "datetime"])  # Sort by store and date
        df["datetime"] = df["datetime"].dt.strftime("%Y-%m-%d")  # Format for JSON serialization
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Data Processing Error: {str(e)}")