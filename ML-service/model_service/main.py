import os
import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from pydantic import BaseModel
from io import StringIO
import mlflow.pyfunc
from mlflow.tracking import MlflowClient

from utils import get_predictors
from analytics import router as analytics_router

model = None
model_metadata = {
    "run_id": None,
    "name": None,
    "version": None
}

# Dictionary to cache multiple store-specific models
model_cache = {}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manages the FastAPI application lifecycle and global model state.

    On startup, this function configures the MLflow environment, connects to the 
    tracking server, and attempts to load the current 'Production' model into memory. 
    It also populates global metadata by cross-referencing the loaded model's 
    run ID with the MLflow Model Registry.

    Args:
        app (FastAPI): The FastAPI application instance.

    Yields:
        None: Control is yielded back to the FastAPI framework to start the server.
    """
    os.environ["MLFLOW_HTTP_REQUEST_TIMEOUT"] = "60"
    os.environ["MLFLOW_ALLOW_HTTP_REDIRECTS"] = "true"

    MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://mlflow:5080")
    ORIGINAL_MODEL_URI="models:/daily_sales_forecast_model/Production"

    print(f"Connecting to MLflow at {MLFLOW_TRACKING_URI}...")
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    try: 
        global model 
        model = mlflow.pyfunc.load_model(ORIGINAL_MODEL_URI)
        
        model_metadata["run_id"] = model.metadata.run_id

        for rm in client.search_registered_models():
            for mv in rm.latest_versions:
                if mv.run_id == model_metadata["run_id"]:
                    print("Model name:", rm.name)
                    model_metadata["name"] = rm.name 
                    print("Version:", mv.version)
                    model_metadata["version"] = mv.version
                    print("Stage", mv.current_stage)

        print("Original Champion model loaded", flush=True)
    except Exception as e:
        print(f"Failed to load original model: {e}")
        model_metadata.clear()
    yield
    model_cache.clear()


app = FastAPI(lifespan=lifespan)
app.include_router(analytics_router)

class PredictionInput(BaseModel):
    """
    Schema for incoming prediction requests.

    Attributes:
        input_data (str): JSON-serialized string containing the feature set.
    """
    input_data: str 

class StorePredictionInput(BaseModel):
    """
    Schema for incoming prediction requests.

    Attributes:
        input_data (str): JSON-serialized string containing the feature set.
    """
    input_data: str 
    location_id: str

# --- HELPER FOR NEW MODELS ---
def load_store_model(location_id: str):
    """Lazy loads store-specific models into the cache."""
    if location_id in model_cache:
        return model_cache[location_id]
    
    model_name = f"store_{location_id}_forecast_model"
    model_uri = f"models:/{model_name}/Production"
    client = MlflowClient()

    try:
        print(f"Loading Production model for store {location_id}...")
        loaded_store_model = mlflow.pyfunc.load_model(model_uri)
        latest_versions = client.get_latest_versions(model_name, stages=["Production"])
        version = latest_versions[0].version if latest_versions else "unknown"

        model_cache[location_id] = {
            "model": loaded_store_model,
            "name": model_name,
            "version": version
        }
        return model_cache[location_id]
    except Exception as e:
        print(f"Error loading model for store {location_id}: {e}")
        return None

# --- ENDPOINTS ---

@app.post("/predict_sales")
async def predict(request: PredictionInput):
    """
    Processes features and returns a model prediction.

    Converts the input JSON string into a DataFrame, standardizes the index 
    and feature set, and executes a prediction using the global champion model.

    Args:
        request (PredictionInput): The container for the raw input data.

    Returns:
        JSONResponse: A dictionary containing the prediction, timestamp, 
                      and source model metadata.
    """
    input_df = pd.read_json(StringIO(request.input_data))
    input_df = input_df.set_index('datetime')

    predictors = get_predictors(input_df)
    X = input_df[predictors]

    prediction = model.predict(X)

    output = {
        "datetime": str(input_df.index[0]), 
        "prediction": prediction[0].item(),
        "model_name": model_metadata["name"],
        "model_version": model_metadata["version"]
    }

    return JSONResponse(content=output)


@app.post("/predict_store_sales")
async def predict_store(request: StorePredictionInput):
    store_meta = load_store_model(request.location_id)
    if not store_meta:
        raise HTTPException(status_code=404, detail=f"Model for store {request.location_id} not found.")

    input_df = pd.read_json(StringIO(request.input_data))
    if 'datetime' in input_df.columns:
        input_df = input_df.set_index('datetime')
    
    X = input_df[get_predictors(input_df)]
    prediction = store_meta["model"].predict(X)

    return JSONResponse(content={
        "location_id": request.location_id,
        "datetime": str(input_df.index[0]),
        "prediction": float(prediction[0]),
        "model_name": store_meta["name"],
        "model_version": store_meta["version"]
    })