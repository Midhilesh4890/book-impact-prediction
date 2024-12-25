# eda.py

import logging
import os
import tempfile
from pyspark.sql.functions import mean, max, min, count


def _auto_fix_csv_header_in_memory(csv_path):
    lines = []
    with open(csv_path, "r", encoding="utf-8") as f:
        lines = f.readlines()

    if not lines:
        logging.warning(f"{csv_path} is empty.")
        return csv_path

    header = lines[0].rstrip("\n")
    if header.startswith(","):
        fixed_header = "id" + header
        lines[0] = fixed_header + "\n"
        logging.info("Auto-fixed CSV header in memory.")
        tmp = tempfile.NamedTemporaryFile(
            mode="w", delete=False, suffix=".csv")
        tmp.writelines(lines)
        tmp.flush()
        tmp.close()
        return tmp.name

    return csv_path


def perform_eda(spark, dataset_path):
    try:
        logging.info(f"Loading dataset from {dataset_path}...")

        if not os.path.exists(dataset_path):
            logging.warning(f"{dataset_path} does not exist.")
            return None

        fixed_path = _auto_fix_csv_header_in_memory(dataset_path)

        df = (
            spark.read
                 .option("header", "true")
                 .option("inferSchema", "true")
                 .option("enforceSchema", "false")
                 .csv(fixed_path)
        )

        if "_c0" in df.columns:
            df = df.withColumnRenamed("_c0", "id")

        logging.info("Schema:")
        df.printSchema()

        logging.info("First 5 rows:")
        df.show(5)

        logging.info("Summary statistics:")
        df.describe().show()

        if "categories" in df.columns and "Impact" in df.columns:
            logging.info("Category-wise impact summary:")
            df.groupBy("categories").agg(
                count("*").alias("Total Books"),
                mean("Impact").alias("Average Impact"),
                max("Impact").alias("Max Impact"),
                min("Impact").alias("Min Impact")
            ).show()
        else:
            logging.warning(
                "Missing 'categories' or 'Impact'; skipping groupBy.")

        return df

    except Exception as e:
        logging.error(f"EDA failed: {e}")
        raise
