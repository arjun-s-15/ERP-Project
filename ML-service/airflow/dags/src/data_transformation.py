import pandas as pd
import numpy as np 
from abc import ABC, abstractmethod

class DataTransformationTemplate(ABC):
    """
    Abstract base class defining the skeletal structure for data transformation.

    Args:
        df (pd.DataFrame): The input dataframe to be transformed.

    Methods:
        apply_transformation: Orchestrates the pipeline execution order.
        reorder_data: Abstract method for data type and index normalization.
        derive_indicators: Abstract method for feature engineering.
        clean_data: Abstract method for handling missing values and duplicates.
    """
    def __init__(self, df):
        self.df = df 

    def apply_transformation(self) -> pd.DataFrame:
        """
        Executes the transformation steps in a fixed algorithmic sequence.

        Returns:
            pd.DataFrame: The final processed dataset.
        """
        raw_df = self.reorder_data(self.df)
        transformed_df = self.derive_features(raw_df)
        cleaned_df = self.clean_data(transformed_df)
        return cleaned_df 

    @abstractmethod
    def reorder_data(self, df):
        """
        Normalizes types and structure. Implementation required by subclass.
        """
        pass 

    @abstractmethod
    def derive_features(self, df):
        """
        Applies domain-specific logic. Implementation required by subclass.
        """
        pass 

    @abstractmethod
    def clean_data(self, df):
        """
        Finalizes dataset for output. Implementation required by subclass.
        """
        pass 


class DailySalesDataTransformation(DataTransformationTemplate):

    def reorder_data(self, df: pd.DataFrame):
        """
        Standardizes the DataFrame structure for time-series analysis.

        Args:
            df (pd.DataFrame): Raw input data from the database.

        Returns:
            pd.DataFrame: Processed DataFrame with a sorted DatetimeIndex.
        """
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="raise")

        df = df.rename(columns={"event_timestamp": "datetime"})
        df = df.sort_values(by='datetime')
        df["quantity"] = df["quantity"].astype("float64")

        df_daily = (
            df.groupby("datetime", as_index=False)['quantity']
            .sum()
            .rename(columns={"quantity": "total_sales"})
        ) 
        df_daily = (
            df_daily
            .sort_values(by="datetime")
            .set_index("datetime")
            .asfreq("D")
        )
        df_daily["total_sales"] = df_daily["total_sales"].fillna(0)
        df_daily = df_daily.reset_index()

        return df_daily

    def derive_features(self, df: pd.DataFrame):
        """
        Performs feature engineering to generate volatility and momentum indicators.

        Args:
            df (pd.DataFrame): The reordered price data.

        Returns:
            pd.DataFrame: DataFrame enriched with log returns, rolling volatility, and Z-scores.
        """
        # Calendar features
        df["day_of_week"] = df["datetime"].dt.dayofweek.astype("int64")
        df["month"] = df["datetime"].dt.month.astype("int64")
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype("int64")

        # Lag features
        df["lag_1"] = df["total_sales"].shift(1).astype("float64")
        df["lag_7"] = df["total_sales"].shift(7).astype("float64")

        # Rolling window features
        df["rolling_mean_7"] = df["total_sales"].rolling(7).mean().astype("float64")

        return df

    def clean_data(self, df: pd.DataFrame):
        """
        Finalizes the dataset by removing incomplete rows resulting from rolling windows.

        Args:
            df (pd.DataFrame): The feature-enriched DataFrame.

        Returns:
            pd.DataFrame: A clean, indexed DataFrame ready for staging.
        """
        df = df.dropna().reset_index(drop=True)

        return df 


class StoreSalesForecastDataTransformation(DataTransformationTemplate):

    def reorder_data(self, df: pd.DataFrame):
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="raise")

        df = df.rename(columns={"event_timestamp": "datetime"})
        df["quantity"] = df["quantity"].astype("float64")

        store_df = df.groupby(['datetime', 'location_id']).agg({'quantity': 'sum'}).reset_index()
        store_df = store_df.sort_values(['location_id', 'datetime'])
        return store_df

    def derive_features(self, df: pd.DataFrame):
        # Calendar Features
        df["day_of_week"] = df["datetime"].dt.day_of_week.astype("int64")
        df["month"] = df["datetime"].dt.month.astype("int64")
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype("int64")

        # Lag Features
        df['lag_1'] = df.groupby('location_id')['quantity'].shift(1)
        df['lag_7'] = df.groupby('location_id')['quantity'].shift(7)

        # Rolling window features
        df['rolling_mean_7'] = df.groupby('location_id')['quantity'].transform(lambda x: x.rolling(window=7).mean())

        return df
    
    def clean_data(self, df: pd.DataFrame):
        df = df.dropna()
        return df

class StoreSalesAnalyticsDataTransformation(DataTransformationTemplate):
    def reorder_data(self, df: pd.DataFrame):
        if "event_timestamp" in df.columns:
            df["event_timestamp"] = pd.to_datetime(df["event_timestamp"], errors="raise")

        df = df.rename(columns={"event_timestamp": "datetime"})
        df["quantity"] = df["quantity"].astype("float64")

        store_df = df.groupby(['datetime', 'location_id']).agg({'quantity': 'sum'}).reset_index()
        store_df = store_df.sort_values(['location_id', 'datetime'])
        return store_df

    def derive_features(self, df: pd.DataFrame):

        # Calendar features
        df["day_of_week"] = df["datetime"].dt.day_of_week.astype("int64")
        df["month"] = df["datetime"].dt.month.astype("int64")
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype("int64")

        return df

    def clean_data(self, df: pd.DataFrame):
        return df.dropna()
    

if __name__ == "__main__": 
    # Load the raw dataset
    raw_df = pd.read_parquet("../../../datasets/transformed_sample_dataset_6m.parquet")
    
    print("--- Initial Raw Dataframe ---")
    print(raw_df.head())
    print(raw_df.info())
    print("\n" + "="*50 + "\n")

    # 1. Existing Daily Sales Transformation
    print("Running: Daily Sales Transformation...")
    daily_transformer = DailySalesDataTransformation(raw_df.copy())
    daily_df = daily_transformer.apply_transformation()
    
    print("Daily Sales Transformation Result:")
    print(daily_df.head())
    print(daily_df.info())
    print("\n" + "-"*30 + "\n")

    # 2. Store Sales Analytics Transformation
    print("Running: Store Sales Analytics Transformation...")
    analytics_transformer = StoreSalesAnalyticsDataTransformation(raw_df.copy())
    
    # Executing the pipeline steps
    analytics_df = analytics_transformer.apply_transformation()
    
    print("Store Analytics Result (Aggregated by Store):")
    print(analytics_df.head())
    print(analytics_df.info())
    print("\n" + "-"*30 + "\n")

    # 3. Store Sales Forecast Transformation
    print("Running: Store Sales Forecast Transformation...")
    forecast_transformer = StoreSalesForecastDataTransformation(raw_df.copy())
    
    # Executing the pipeline steps
    forecast_df = forecast_transformer.apply_transformation()
    
    print("Store Forecast Result (Includes Lags & Rolling Windows):")
    print(forecast_df.head())
    print(forecast_df.info())
    