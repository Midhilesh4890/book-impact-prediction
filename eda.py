# eda.py

import logging
from pyspark.sql.functions import mean, max, min, count


def perform_eda(spark, dataset_path):
    """Performs EDA on the dataset using Spark DataFrame operations."""
    try:
        logging.info(f"Loading dataset for EDA from {dataset_path}...")
        df = spark.read.csv(dataset_path, header=True, inferSchema=True)

        logging.info("Dataset Schema:")
        df.printSchema()

        logging.info("First 5 rows:")
        df.show(5)

        logging.info("Summary statistics for numeric columns:")
        df.describe().show()

        logging.info("Category-wise impact summary (grouped by 'categories'):")
        df.groupBy("categories").agg(
            count("*").alias("Total Books"),
            mean("Impact").alias("Average Impact"),
            max("Impact").alias("Max Impact"),
            min("Impact").alias("Min Impact")
        ).show()

        return df
    except Exception as e:
        logging.error(f"EDA failed: {e}")
        raise
