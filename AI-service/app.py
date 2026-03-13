import os
from dotenv import load_dotenv
from time import time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from graphs import graph

# --- Load Environment Variables ---
load_dotenv()

# --- FastAPI App Setup ---
app = FastAPI()

@app.post("/run-graph")
async def run_graph(input_file_path: str) -> str:
    result = graph.invoke(input={"file_path": input_file_path}) # type: ignore
    return result["transformed_df"]


# if __name__ == "__main__":
#     input_file_path = "sample_data/online_retail_09_10.csv"
#     result = graph.invoke({"file_path": input_file_path}) # type: ignore
#     df = result["transformed_df"]
#     print(df.head())