# utils.py

import logging
import os
from pyspark.sql import SparkSession


def create_spark_session(app_name):
    """
    Creates a Spark session with the specified application name.
    """
    try:
        spark = SparkSession.builder.appName(app_name).getOrCreate()
        logging.info(f"Spark session created with app name: {app_name}")
        return spark
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        raise


def configure_logging(log_file_path=None):
    """
    Configures logging.
    If `log_file_path` is provided, logs will be written to that file.
    Otherwise, a default log file named 'default.log' will be created
    inside a 'logs' directory.
    """
    # If log_file_path is not provided, create a default logs directory and file
    if not log_file_path:
        os.makedirs("logs", exist_ok=True)  # Make sure 'logs' directory exists
        log_file_path = os.path.join("logs", "default.log")

    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format=log_format,
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    logging.info(f"Logging is configured. Writing logs to: {log_file_path}")
