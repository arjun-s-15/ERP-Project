from abc import ABC, abstractmethod
import pandas as pd

class DataPreProcessingTemplate(ABC):
    """
    An abstract base class defining the workflow for preparing datasets for machine learning.

    Args:
        df (pd.DataFrame): The input dataset (typically from the 'final' production table).

    Methods:
        preprocess_data: Orchestrates the target derivation and data splitting sequence.
        derive_target: Abstract method to define the label/y-variable.
        split_dataset: Abstract method to divide data into training and evaluation sets.
    """
    def __init__(self, df):
        self.df = df

    def preprocess_data(self):
        """
        Executes the preprocessing pipeline in a fixed order.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: A tuple containing the (train_df, test_df).
        """
        processed_df = self.derive_target(self.df)
        train_df, test_df = self.split_dataset(processed_df)
        return train_df, test_df

    @abstractmethod
    def derive_target(self, df):
        """
        Logic to create the ground truth labels. Implementation required by subclass.
        """
        pass 
    
    @abstractmethod
    def split_dataset(self, df):
        """
        Logic to partition data (e.g., Random split or Time-series split). 
        Implementation required by subclass.
        """
        pass 


class DailySalesDataPreProcessing(DataPreProcessingTemplate):
    """
    Concrete implementation of preprocessing logic for Forex binary classification.

    Methods:
        derive_target: Creates a binary target indicating if the price will drop tomorrow.
        split_dataset: Partitions the data into training and testing sets using index slicing.
    """
    def derive_target(self, df: pd.DataFrame):
        """
        Calculates the 'target' column based on the next day's closing price.

        Args:
            df (pd.DataFrame): The feature-enriched price data.

        Returns:
            pd.DataFrame: DataFrame with the 'target' binary label.
        """
        df["target"] = df["total_sales"].shift(-1)
        df = df.dropna().reset_index(drop=True)
        return df
    
    def split_dataset(self, df: pd.DataFrame):
        """
        Splits data into train and test sets based on row position.

        Args:
            df (pd.DataFrame): The labeled dataset.

        Returns:
            Tuple[pd.DataFrame, pd.DataFrame]: (train_df, test_df)
        """
        split_idx = int(len(df) * 0.8)
        test_df = df.iloc[split_idx:]
        train_df = df.iloc[:split_idx]

        return train_df, test_df
    

class StoreSalesDataPreProcessing(DataPreProcessingTemplate):
    def derive_target(self, df: pd.DataFrame):
        df = df.rename(columns={"quantity": "total_sales"})
        df = df.sort_values(by="datetime")
        df["target"] = df["total_sales"].shift(-1)
        df = df.dropna().reset_index(drop=True)
        df = df.drop(columns=["location_id"])
        return df
    
    def split_dataset(self, df: pd.DataFrame):
        split_idx = int(len(df) * 0.8)
        test_df = df.iloc[split_idx:]
        train_df = df.iloc[:split_idx]

        return train_df, test_df

if __name__ == "__main__":
   # --- 1. Processing Daily Total Sales (Global) ---
    df = pd.read_parquet("../../../datasets/daily_total_sales.parquet")
    print("--- GLOBAL DAILY SALES ---")
    
    processor_daily = DailySalesDataPreProcessing(df)
    train_df_daily, test_df_daily = processor_daily.preprocess_data()
    
    print(f"Train Shape: {train_df_daily.shape}")
    print(f"Test Shape: {test_df_daily.shape}")
    print("-" * 30)


    # --- 2. Processing Store Sales Forecast (Per Location) ---
    store_df = pd.read_parquet("../../../datasets/store_sales_forecast.parquet")
    locations = store_df['location_id'].unique()

    print(f"--- PER-LOCATION PREPROCESSING ({len(locations)} Locations Found) ---")

    for loc_id in locations:
        print(f"\n{'='*15} LOCATION: {loc_id} {'='*15}")

        # Isolate location data
        loc_subset = store_df[store_df['location_id'] == loc_id].copy()

        # Initialize processor for this specific location
        processor = StoreSalesDataPreProcessing(loc_subset)
        loc_train, loc_test = processor.preprocess_data()

        # --- TRAIN DIAGNOSTICS ---
        print(f"\n[TRAIN DATA - {loc_id}]")
        print(f"Shape: {loc_train.shape}")
        print("Head:")
        print(loc_train.head(3))
        print(loc_train.info())

        # --- TEST DIAGNOSTICS ---
        print(f"\n[TEST DATA - {loc_id}]")
        print(f"Shape: {loc_test.shape}")
        print("Head:")
        print(loc_test.head(3))
        print(loc_test.info())

    print(f"\n{'*'*40}")
    print(f"Finished processing Location {loc_id}")