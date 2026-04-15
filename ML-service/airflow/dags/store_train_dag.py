from datetime import datetime, timedelta
import io
from typing import List
from airflow.sdk import dag, task 
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from dotenv import load_dotenv
from src.data_preprocessing import StoreSalesDataPreProcessing
from src.orchestrator import TrainingOrchestrator 
from src.model_tuning import OptunaModelTuner 
from src.model_promotion import ModelPromotionManager 
import pandas as pd

load_dotenv()

BUCKET_NAME = "insighto-s3-bucket"
FILE_KEY = "data/store_sales_forecast.parquet"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/048013208170/InsightoQueue"

default_args = {
    'owner': 'atharv',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
        dag_id="store_train_pipeline",
        default_args=default_args,
        start_date=datetime(2026, 3, 24)
    )
def store_train_pipeline():
    """
    Model training pipeline dag. Trains new models and compare with production model and promote the best model.
    """
    @task(multiple_outputs=True)
    def data_preprocessing():
        """
        Orchestrates the preparation and storage of training and testing datasets, partitioned by location_id.

        Args:
            None

        Returns:
            dict: A mapping of the created table names (e.g., {'train_dataset': 's3_file_key', ...}) 
                or None if an error occurs.
        """
        try:
            s3 = S3Hook(aws_conn_id="aws_default")

            # 1. Retreive raw data
            file_obj = s3.get_key(FILE_KEY, bucket_name=BUCKET_NAME)
            body = file_obj.get()["Body"].read()
            buffer = io.BytesIO(body)  
            del body
            input_df = pd.read_parquet(buffer)
            del buffer
            input_df.columns = input_df.columns.str.strip()
            print("Input data retrieved: ", FILE_KEY)

            location_datasets = {}
            locations = input_df['location_id'].unique()

            # 2. Process each location individually
            for loc_id in locations:
                print(f"Processing location: {loc_id}")

                # Isolate and Preprocess
                loc_subset = input_df[input_df['location_id'] == loc_id].copy()
                preprocessor = StoreSalesDataPreProcessing(loc_subset)
                train_df, test_df = preprocessor.preprocess_data()

                # Define unique S3 keys
                s3_train_key = f"data/store_{loc_id}_train.parquet"
                s3_test_key = f"data/store_{loc_id}_test.parquet"

                # 3. Upload pairs to S3
                for df, key in [(train_df, s3_train_key), (test_df, s3_test_key)]:
                    buffer = io.BytesIO()
                    df.to_parquet(buffer, index=False)
                    buffer.seek(0)

                    s3.get_conn().put_object(
                        Bucket=BUCKET_NAME,
                        Key=key,
                        Body=buffer.getvalue()
                    )
                    del buffer

                # 4. Track keys for this location
                location_datasets[str(loc_id)] = {
                    "train_path": s3_train_key,
                    "test_path": s3_test_key,
                }

            # Return a structured map of all locations and their respective file paths
            return {"location_metadata": location_datasets}

        except Exception as e:
            print(f"Error in data_preprocessing: {e}")
            raise

    @task(multiple_outputs=True)
    def model_training(location_datasets: dict):
        """
        Loads training data from the database and executes the model training pipeline.

        Args:
            datasets (dict): A dictionary containing the 'train_dataset' file name.

        Returns:
            dict: A dictionary containing the best model's performance metrics, 
                parameters, and metadata.
        """
        s3 = S3Hook(aws_conn_id="aws_default")

        # Extract the actual metadata map from the XCom dictionary
        location_map = location_datasets.get("location_metadata", {})
        all_best_models = {}

        if not location_map:
            print("No location metadata found to process.")
            return {}

        for loc_id, paths in location_map.items():
            train_key = paths["train_path"]

            # Construct the dynamic model name based on the dict key
            model_name = f"store_{loc_id}_forecast_model"

            print(f"\n--- Starting Training for Location: {loc_id} ---")
            print(f"Target Model Registry: {model_name}")

            # 1. Retrieve the specific parquet file for this store
            file_obj = s3.get_key(train_key, bucket_name=BUCKET_NAME)
            body = file_obj.get()["Body"].read()
            buffer = io.BytesIO(body)  
            del body 
            input_df = pd.read_parquet(buffer)
            del buffer
            input_df.columns = input_df.columns.str.strip()

            # 2. Run the Orchestrator for this specific store
            orchestrator = TrainingOrchestrator(df=input_df, registered_model_name=model_name)
            best_model_dict = orchestrator.run()

            # 3. Tag the result with the location_id for downstream tracking
            best_model_dict["location_id"] = loc_id
            all_best_models[loc_id] = best_model_dict

            print(f"Completed {loc_id}. Architecture selected: {best_model_dict.get('model_type')}")
        
        
        return all_best_models
        
    @task(multiple_outputs=True)
    def hyperparameter_tuning(datasets, best_model_data):
        """
        Performs hyperparameter optimization on the selected model.

        Args:
            datasets (dict): Dictionary containing the 'train_dataset' table name.
            best_model_data (dict): Metadata and configuration of the model selected 
                                    for tuning.

        Returns:
            dict: A report containing the optimized hyperparameters and updated 
                model metrics.
        """
        train_key = datasets["train_dataset"]
        s3 = S3Hook(aws_conn_id="aws_default")

        file_obj = s3.get_key(train_key, bucket_name=BUCKET_NAME)
        body = file_obj.get()["Body"].read()
        buffer = io.BytesIO(body)  
        del body
        train_df = pd.read_parquet(buffer)
        del buffer
        train_df.columns = train_df.columns.str.strip()
        tuner = OptunaModelTuner(train_df, best_model_data)
        tuned_model_data = tuner.start_tuning()
        print("Tuned model report: ")
        print(tuned_model_data)
        return tuned_model_data
    
    @task(multiple_outputs=True)
    def train_challenger(datasets, tuned_model_data):
        """
        Trains a final 'challenger' model using the optimized hyperparameters.

        Args:
            datasets (dict): Dictionary containing the 'train_dataset' table name.
            tuned_model_data (dict): The optimized hyperparameter configuration 
                                    retrieved from the tuning phase.

        Returns:
            dict: A dictionary containing the challenger model's path, 
                performance metrics, and metadata.
        """
        train_key = datasets["train_dataset"]
        s3 = S3Hook(aws_conn_id="aws_default")

        file_obj = s3.get_key(train_key, bucket_name=BUCKET_NAME)
        body = file_obj.get()["Body"].read()
        buffer = io.BytesIO(body)  
        del body
        train_df = pd.read_parquet(buffer)
        del buffer
        train_df.columns = train_df.columns.str.strip()

        orchestrator = TrainingOrchestrator(train_df)
        challenger_data = orchestrator.train_challenger(tuned_model_data)
        
        print(challenger_data)
        return challenger_data
    
    @task 
    def model_promotion(datasets, challenger_data):
        """
        Evaluates the challenger model against the current champion and promotes if superior.

        Args:
            datasets (dict): Dictionary containing the 'test_dataset' file name.
            challenger_data (dict): Metadata and performance metrics for the candidate model.

        Returns:
            None: Updates the model registry or production alias via ModelPromotionManager.
        """
        test_key = datasets["test_dataset"]
        s3 = S3Hook(aws_conn_id="aws_default")

        file_obj = s3.get_key(test_key, bucket_name=BUCKET_NAME)
        body = file_obj.get()["Body"].read()
        buffer = io.BytesIO(body)  
        del body
        test_df = pd.read_parquet(buffer)
        del buffer
        test_df.columns = test_df.columns.str.strip()
        
        manager = ModelPromotionManager(model_name=challenger_data["name"], test_df=test_df)
        result = manager.promote_if_better()
        print(result)


    datasets = data_preprocessing()
    best_model_data = model_training(datasets)
    # tuned_model_data = hyperparameter_tuning(datasets, best_model_data)
    # challenger_data = train_challenger(datasets, tuned_model_data)
    # model_promotion(datasets, challenger_data)
    
training_pipeline = store_train_pipeline()
