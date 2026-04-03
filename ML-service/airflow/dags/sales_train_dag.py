from datetime import datetime, timedelta
import io
from airflow.sdk import dag, task 
from airflow.providers.amazon.aws.sensors.s3 import S3Hook
from dotenv import load_dotenv
from src.data_preprocessing import DailySalesDataPreProcessing 
from src.orchestrator import TrainingOrchestrator 
from src.model_tuning import OptunaModelTuner 
from src.model_promotion import ModelPromotionManager 
import pandas as pd

load_dotenv()

BUCKET_NAME = "insighto-s3-bucket"
FILE_KEY = "data/daily_total_sales.parquet"
SQS_QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/048013208170/InsightoQueue"

default_args = {
    'owner': 'atharv',
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

@dag(
        dag_id="sales_train_pipeline",
        default_args=default_args,
        start_date=datetime(2026, 3, 24)
    )
def sales_train_pipeline():
    """
    Model training pipeline dag. Trains new models and compare with production model and promote the best model.
    """
    @task(multiple_outputs=True)
    def data_preprocessing():
        """
        Orchestrates the preparation and storage of training and testing datasets.

        Args:
            None

        Returns:
            dict: A mapping of the created table names (e.g., {'train_dataset': 's3_file_key', ...}) 
                or None if an error occurs.
        """
        try:
            s3 = S3Hook(aws_conn_id="aws_default")

            file_obj = s3.get_key(FILE_KEY, bucket_name=BUCKET_NAME)
            body = file_obj.get()["Body"].read()
            buffer = io.BytesIO(body)  
            del body
            input_df = pd.read_parquet(buffer)
            del buffer
            input_df.columns = input_df.columns.str.strip()
            print("Input data retrieved: ", FILE_KEY)

            preprocessor = DailySalesDataPreProcessing(input_df)
            train_df, test_df = preprocessor.preprocess_data()

            # upload train table
            s3_train_key = "data/sales_train_data.parquet"
            s3_test_key = "data/sales_test_data.parquet"

            for df, key in [(train_df, s3_train_key), (test_df, s3_test_key)]:
                buffer = io.BytesIO()
                df.to_parquet(buffer, index=False)
                buffer.seek(0)

                s3.get_conn().put_object(
                    Bucket=BUCKET_NAME,
                    Key=key,
                    Body=buffer.getvalue()
                )
                print(f"Uploaded: s3://{BUCKET_NAME}/{key}")
                del buffer

            return {"train_dataset": s3_train_key, "test_dataset": s3_test_key}

        except Exception as e:
            print(f"Error in data_preprocessing: {e}")
            return None

    @task(multiple_outputs=True)
    def model_training(datasets):
        """
        Loads training data from the database and executes the model training pipeline.

        Args:
            datasets (dict): A dictionary containing the 'train_dataset' file name.

        Returns:
            dict: A dictionary containing the best model's performance metrics, 
                parameters, and metadata.
        """
        train_key = datasets["train_dataset"]
        s3 = S3Hook(aws_conn_id="aws_default")

        file_obj = s3.get_key(train_key, bucket_name=BUCKET_NAME)
        body = file_obj.get()["Body"].read()
        buffer = io.BytesIO(body)  
        del body
        input_df = pd.read_parquet(buffer)
        del buffer
        input_df.columns = input_df.columns.str.strip()
        print("Train data retrieved: ", input_df.head())
        
        orchestrator = TrainingOrchestrator(input_df)
        best_model_dict = orchestrator.run()
        print(best_model_dict)
        return best_model_dict
        
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
    tuned_model_data = hyperparameter_tuning(datasets, best_model_data)
    challenger_data = train_challenger(datasets, tuned_model_data)
    model_promotion(datasets, challenger_data)
    
prediction_pipeline = sales_train_pipeline()
