import os
import boto3
from dotenv import load_dotenv
from time import time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from graphs import graph

# --- Load Environment Variables ---
load_dotenv()

# --- S3 Client Setup ---
s3_client = boto3.client(
    's3',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY'),
    region_name=os.getenv('AWS_REGION')
)
BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

# --- FastAPI App Setup ---
app = FastAPI()

@app.get("/health/s3")
async def test_s3_connection():
    """Test endpoint to verify S3 bucket connectivity."""
    try:
        # Try listing objects in the bucket (lightweight check)
        response = s3_client.list_objects_v2(
            Bucket=BUCKET_NAME,
            MaxKeys=1  # only fetch 1 object, just to verify access
        )
        return {
            "status": "ok",
            "bucket": BUCKET_NAME,
            "accessible": True,
            "object_count": response.get('KeyCount', 0)
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "status": "error",
                "bucket": BUCKET_NAME,
                "accessible": False,
                "error": str(e)
            }
        )

@app.post("/run-graph")
async def run_graph(input_file_path: str):
    try:

        result = graph.invoke(input={"file_path": input_file_path}) # type: ignore
        transformed_df = result["transformed_df"]

        output_filename = f"transformed_{os.path.basename(input_file_path)}"

        csv_buffer = transformed_df.to_csv(index=False)
        s3_client.put_object(
            Bucket=BUCKET_NAME,
            Key=output_filename,
            Body=csv_buffer,
            ContentType='text/csv'
        )

        return {
            "status": "ok",
            "output_file": output_filename,
            "s3_path": f"s3://{BUCKET_NAME}/{output_filename}"
        }
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "error": str(e)}
        )


# if __name__ == "__main__":
#     input_file_path = "sample_data/online_retail_09_10.csv"
#     result = graph.invoke({"file_path": input_file_path}) # type: ignore
#     df = result["transformed_df"]
#     print(df.head())