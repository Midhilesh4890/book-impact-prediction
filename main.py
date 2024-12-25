# main.py

import logging
from config import DATASET_PATH, SPARK_APP_NAME, WORKER_CONFIGS, LOG_FILE_PATH
from utils import create_spark_session, configure_logging
from eda import perform_eda
from preprocess import preprocess_data
from model import train_model


def main():
    """
    Main entry point for the PySpark + XGBoost pipeline.
    """
    try:
        # Configure logging (comment out if you only want console logs)
        # or configure_logging() for console only
        configure_logging(LOG_FILE_PATH)

        # Create Spark session
        spark = create_spark_session(SPARK_APP_NAME)

        # Perform EDA
        df = perform_eda(spark, DATASET_PATH)

        # Preprocess data
        df_preprocessed = preprocess_data(df)

        # Experiment with different worker configurations
        for workers in WORKER_CONFIGS:
            logging.info(f"Simulating {workers} workers...")
            mape, training_time = train_model(df_preprocessed, workers)
            logging.info(
                f"Workers: {workers}, MAPE: {mape:.4f}%, Training Time: {training_time:.2f}s"
            )

        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline encountered an error: {e}")


if __name__ == "__main__":
    main()
