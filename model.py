# model.py

import logging
import time
import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, mean_absolute_percentage_error


def preprocess_for_xgboost(df):
    """
    Prepares a Pandas DataFrame for XGBoost by handling null values and 
    assembling features. 
    """
    try:
        logging.info("Preprocessing data for XGBoost...")

        # Convert PySpark DataFrame to Pandas DataFrame for xgboost usage
        # NOTE: For large data, you may need another approach or
        # use Spark's own ML libraries instead of xgboost.
        pdf = df.toPandas()

        # Example columns: 'Impact' (target) and 'features' (some numeric columns)
        if 'Impact' not in pdf.columns:
            raise ValueError("'Impact' column not found in DataFrame.")
        if 'features' not in pdf.columns:
            # Example: let's just create a dummy 'features' column for demonstration
            pdf['features'] = 0

        # Handle missing values in 'Impact' and 'features'
        pdf['Impact'] = pdf['Impact'].fillna(0)
        pdf['features'] = pdf['features'].fillna(0)

        logging.info("Preprocessing for XGBoost completed.")
        return pdf
    except Exception as e:
        logging.error(f"Error in preprocessing data for XGBoost: {e}")
        raise


def train_model_with_xgboost(df):
    """
    Trains an XGBoost regression model on the 'features' and 'Impact' columns.

    Args:
        df: A Spark DataFrame containing 'Impact' and 'features' columns 
            (or convertible to such).

    Returns:
        tuple: (mape, training_time)
    """
    try:
        logging.info("Training model using XGBoost...")

        # Convert Spark DF to Pandas and handle any needed transformations
        pdf = preprocess_for_xgboost(df)

        # Separate features and target
        # NOTE: This simple example assumes `features` is a single numeric column.
        # In practice, you'd have multiple feature columns that you'd assemble
        # into a feature matrix.
        X = pdf[['features']]
        y = pdf['Impact']

        # Split the data into training and test sets
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=0.2, random_state=42
        )
        logging.info(
            f"Training data size: {X_train.shape}, Test data size: {X_test.shape}"
        )

        # Initialize the XGBoost regressor
        model = xgb.XGBRegressor(
            n_estimators=100,
            max_depth=6,
            learning_rate=0.1,
            objective='reg:squarederror',
            random_state=42
        )

        # Train the model
        start_time = time.time()
        model.fit(X_train, y_train)
        training_time = time.time() - start_time

        logging.info(
            f"Model training completed in {training_time:.2f} seconds."
        )

        # Make predictions
        predictions = model.predict(X_test)

        # Evaluate the model
        mae = mean_absolute_error(y_test, predictions)
        mape = mean_absolute_percentage_error(y_test, predictions) * 100

        logging.info(
            f"Model evaluation completed. MAPE: {mape:.4f}%, MAE: {mae:.4f}"
        )
        return mape, training_time

    except Exception as e:
        logging.error(f"Model training failed: {e}")
        raise


def train_model(df, num_workers):
    """
    A wrapper function to simulate training with a given number of workers.
    For demonstration, we pass `num_workers` but XGBoost might or might not 
    utilize that directly.
    """
    try:
        logging.info(f"Simulating training with {num_workers} workers...")
        # In a real cluster environment, you would distribute training tasks
        # or set XGBoost's n_jobs = num_workers, etc.

        mape, training_time = train_model_with_xgboost(df)
        return mape, training_time
    except Exception as e:
        logging.error(f"Error in train_model: {e}")
        raise
