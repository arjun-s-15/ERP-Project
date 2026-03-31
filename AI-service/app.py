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
LOCAL_INPUT_DIR = "/tmp/input"
LOCAL_OUTPUT_DIR = "/tmp/output"

os.makedirs(LOCAL_INPUT_DIR, exist_ok=True)
os.makedirs(LOCAL_OUTPUT_DIR, exist_ok=True)

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
async def run_graph(input_filename: str):
    try:
        local_input_path = os.path.join(LOCAL_INPUT_DIR, input_filename)

        s3_client.download_file(
            BUCKET_NAME,
            f"input/{input_filename}",
            local_input_path
        )

        output_filename = f"transformed_{input_filename}"
        local_output_path = os.path.join(LOCAL_OUTPUT_DIR, output_filename)

        result = graph.invoke({
            "input_path": local_input_path,
            "output_path": local_output_path
        }) #type: ignore

        transformed_path = result["output_path"]

        s3_output_key = f"data/{output_filename}"

        with open(transformed_path, "rb") as f:
            s3_client.upload_fileobj(
                f,
                BUCKET_NAME,
                s3_output_key
            )

        return {
            "status": "ok",
            "s3_output_path": f"s3://{BUCKET_NAME}/{s3_output_key}"
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