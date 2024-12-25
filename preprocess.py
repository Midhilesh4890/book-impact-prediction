# preprocess.py

import logging
from pyspark.sql.functions import col


def preprocess_data(df):
    """Preprocesses and engineers features from the dataset."""
    try:
        logging.info("Starting data preprocessing...")

        # Drop rows with missing values
        df = df.na.drop()
        logging.info("Dropped rows with missing values.")

        # Convert 'Impact' column to float (example transformation)
        df = df.withColumn("Impact", col("Impact").cast("float"))

        logging.info("Data preprocessing completed.")
        return df
    except Exception as e:
        logging.error(f"Preprocessing failed: {e}")
        raise
