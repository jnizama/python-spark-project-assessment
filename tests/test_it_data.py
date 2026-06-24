from pyspark.sql import SparkSession
from chispa import assert_df_equality

import sys
import os

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            ".."
        )
    )
)

from src.it_data import generate_it_data


def test_generate_it_data():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    df_one = spark.createDataFrame(
        [
            (1, "IT", 100, 80),
            (2, "HR", 100, 70),
            (3, "IT", 100, 90)
        ],
        [
            "id",
            "area",
            "calls_made",
            "calls_successful"
        ]
    )

    df_two = spark.createDataFrame(
        [
            (1, "John", "Addr1", 1000.0),
            (2, "Mary", "Addr2", 2000.0),
            (3, "Peter", "Addr3", 3000.0)
        ],
        [
            "id",
            "name",
            "address",
            "sales_amount"
        ]
    )

    actual_df = generate_it_data(
        df_one,
        df_two
    )

    expected_df = spark.createDataFrame(
        [
            (3, "IT", 100, 90, "Peter", "Addr3", 3000.0),
            (1, "IT", 100, 80, "John", "Addr1", 1000.0)
        ],
        [
            "id",
            "area",
            "calls_made",
            "calls_successful",
            "name",
            "address",
            "sales_amount"
        ]
    )

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False
    )

    spark.stop()