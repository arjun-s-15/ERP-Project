import pandas as pd


def get_predictors(df: pd.DataFrame):
    """
    Identifies the feature set (X) by excluding target variable

    Args:
        df (pd.DataFrame): The preprocessed dataset.

    Returns:
        list: A list of column names to be used as model inputs.
    """
    feature_cols = df.columns
    return list(feature_cols)