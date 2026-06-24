import argparse
from pyspark.sql import SparkSession
import logging
# Adding module to basic checking
from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract

logger = logging.getLogger(__name__)
# Goal: See all the schema from the source file
# Author: Jorge Nizama
# Date: 18 June, 2026 

# important function to checking

def run_basic_data_quality_checks(
    df_one,
    df_two,
    df_three
):
    #print("\n===== DATA QUALITY CHECKS =====")
    logger.info("DATA QUALITY CHECKS")

    # Expected row counts
    logger.info("Checking row counts...")

    logger.info(
        f"dataset_one: {df_one.count()} "
        f"(expected 1000)"
    )

    logger.info(
        f"dataset_two: {df_two.count()} "
        f"(expected 1000)"
    )

    logger.info(
        f"dataset_three: {df_three.count()} "
        f"(expected 10000)"
    )

    # Null IDs
    logger.info("Checking null IDs...")

    logger.info(
        f"dataset_one null ids: "
        f"{df_one.filter(col('id').isNull()).count()}"
    )

    logger.info(
        f"dataset_two null ids: "
        f"{df_two.filter(col('id').isNull()).count()}"
    )

    logger.info(
        f"dataset_three null ids: "
        f"{df_three.filter(col('id').isNull()).count()}"
    )

    # Unique IDs
    logger.info("Checking unique IDs...")

    logger.info(
        f"dataset_one unique ids: "
        f"{df_one.count() == df_one.select('id').distinct().count()}"
    )

    logger.info(
        f"dataset_two unique ids: "
        f"{df_two.count() == df_two.select('id').distinct().count()}"
    )

    logger.info(
        f"dataset_three unique ids: "
        f"{df_three.count() == df_three.select('id').distinct().count()}"
    )

    # Validation for: numerical fields should not be lower than 0
    logger.info("Checking numeric fields >= 0...")

    logger.info(
        f"dataset_one negative calls_made: "
        f"{df_one.filter(col('calls_made') < 0).count()}"
    )

    logger.info(
        f"dataset_one negative calls_successful: "
        f"{df_one.filter(col('calls_successful') < 0).count()}"
    )

    logger.info(
        f"dataset_two negative sales_amount: "
        f"{df_two.filter(col('sales_amount') < 0).count()}"
    )

    logger.info(
        f"dataset_three negative age: "
        f"{df_three.filter(col('age') < 0).count()}"
    )

    logger.info(
        f"dataset_three negative quantity: "
        f"{df_three.filter(col('quantity') < 0).count()}"
    )

    # Referential Integrity
    logger.info("Checking referential integrity...")

    invalid_caller_ids = (
        df_three
        .join(
            df_one,
            df_three.caller_id == df_one.id,
            "left_anti"
        )
        .count()
    )

    logger.info(
        f"dataset_three invalid caller_ids: "
        f"{invalid_caller_ids}"
    )

    # Calls consistency
    logger.info("Checking calls consistency...")

    invalid_calls = (
        df_one
        .filter(
            col("calls_successful")
            > col("calls_made")
        )
        .count()
    )

    logger.info(
        f"calls_successful > calls_made: "
        f"{invalid_calls}"
    )

    # Address format
    logger.info("Checking address format...")

    zipcode_pattern = r"\d{4}\s[A-Z]{2}"

    invalid_addresses = (
        df_two
        .filter(
            regexp_extract(
                col("address"),
                zipcode_pattern,
                0
            ) == ""
        )
        .count()
    )

    logger.info(
        f"invalid addresses: "
        f"{invalid_addresses}"
    )