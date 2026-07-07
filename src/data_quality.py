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

def validate_condition(
    condition: bool,
    success_message: str,
    warning_message: str
) -> None:
    """
    Log validation results.

    Parameters
    ----------
    condition
        Validation result.

    success_message
        Message written when validation succeeds.

    warning_message
        Message written when validation fails.
    """

    if condition:
        logger.info(success_message)
    else:
        logger.warning(warning_message)

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

    dataset_one_count = df_one.count()
    validate_condition(
        dataset_one_count == 1000,
        f"dataset_one row count OK ({dataset_one_count})",
        f"dataset_one row count INVALID ({dataset_one_count}) expected 1000"
    )

    dataset_two_count = df_two.count()

    validate_condition(
        dataset_one_count == 1000,
        f"dataset_one row count OK ({dataset_two_count})",
        f"dataset_one row count INVALID ({dataset_two_count}) expected 1000"
    )


    dataset_three_count = df_three.count()

    validate_condition(
        dataset_three_count == 1000,
        f"dataset_one row count OK ({dataset_three_count})",
        f"dataset_one row count INVALID ({dataset_three_count}) expected 1000"
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

    unique = ( df_one.count() == df_one.select("id").distinct().count() )
    validate_condition(
        unique,
        "dataset_one unique ids OK",
        "dataset_one duplicated ids found"
    )


    unique = ( df_two.count() == df_two.select("id").distinct().count() )
    validate_condition(
        unique,
        "dataset_two unique ids OK",
        "dataset_two duplicated ids found"
    )
    
    unique = ( df_three.count() == df_three.select("id").distinct().count() )
    validate_condition(
        unique,
        "dataset_three unique ids OK",
        "dataset_three duplicated ids found"
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
    validate_condition(
        invalid_calls == 0,
        "calls consistency OK",
        f"{invalid_calls} records have calls_successful > calls_made"
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
    validate_condition(
        invalid_addresses == 0,
        "Address format consistency OK",
        f"{invalid_addresses} records have calls_successful > calls_made"
    )