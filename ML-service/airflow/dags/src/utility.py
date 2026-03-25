import numpy as np
import pandas as pd
from sklearn.metrics import mean_absolute_error, mean_squared_error
from sklearn.model_selection import TimeSeriesSplit


def get_predictors(df: pd.DataFrame):
    """
    Identifies the feature set (X) by excluding target variable

    Args:
        df (pd.DataFrame): The preprocessed dataset.

    Returns:
        list: A list of column names to be used as model inputs.
    """
    feature_cols = df.columns.drop(["target"])
    return feature_cols


def cross_validate_model(regressor, X, y):
    """
    Performs Time-Series Cross-Validation to evaluate model stability over time.

    Using a walk-forward approach, this function splits the data into sequential 
    folds, ensuring no future data is used to predict the past. It calculates 
    the Precision-Recall AUC for each fold.

    Args:
        classifier: A model instance from the ModelFactory with a fit/predict_proba interface.
        X (pd.DataFrame): Feature set.
        y (pd.Series): target feature.

    Returns:
        Tuple[float, float]: The mean and standard deviation of the PR-AUC scores.
    """
    tscv = TimeSeriesSplit(n_splits=5)
    cv_rmse = []
    cv_mae = []

    for fold, (train_idx, val_idx) in enumerate(tscv.split(X)):
        X_fold_tr, X_fold_val = X.iloc[train_idx], X.iloc[val_idx]
        y_fold_tr, y_fold_val = y.iloc[train_idx], y.iloc[val_idx]

        regressor.fit(X_fold_tr, y_fold_tr)
        y_fold_pred = regressor.predict(X_fold_val)

        fold_rmse = np.sqrt(mean_squared_error(y_fold_val, y_fold_pred))
        fold_mae = mean_absolute_error(y_fold_val, y_fold_pred)

        cv_rmse.append(fold_rmse)
        cv_mae.append(fold_mae)
        print(f"Fold {fold + 1} — RMSE: {fold_rmse:,.2f} | MAE: {fold_mae:,.2f}")
    print(f"\nCV Mean RMSE : {np.mean(cv_rmse):,.2f} ± {np.std(cv_rmse):,.2f}")
    print(f"CV Mean MAE  : {np.mean(cv_mae):,.2f} ± {np.std(cv_mae):,.2f}")
    
    return np.mean(cv_rmse), np.std(cv_rmse)
