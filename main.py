from utils import create_spark_session
from model import train_model
from preprocess import preprocess_data
from eda import perform_eda
import logging

import os

# Ensure the logs directory exists
os.makedirs("/app/logs", exist_ok=True)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("/app/logs/pipeline.log"),  # Save logs to file
        logging.StreamHandler()  # Print logs to console
    ]
)

logger = logging.getLogger(__name__)

WORKER_CONFIGS = [1, 2, 4]


def main():
    try:
        logging.info("Starting the PySpark pipeline...")

        # Step 1: Create Spark session
        spark = create_spark_session("BookImpactPipeline")

        # Step 2: Preprocess data
        file_path = "/app/data/books_task.csv"
        df = perform_eda(spark, file_path)
        df = preprocess_data(df)

        # Experiment with different worker configurations
        for workers in WORKER_CONFIGS:
            logging.info(f"Simulating {workers} workers...")
            mape, training_time = train_model(df, workers)
            logging.info(
                f"Workers: {workers}, MAPE: {mape:.4f}%, Training Time: {training_time:.2f}s"
            )

        logging.info("Pipeline completed successfully.")
    except Exception as e:
        logging.error(f"Pipeline encountered an error: {e}")
        raise


if __name__ == "__main__":
    main()
