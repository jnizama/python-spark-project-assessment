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

from src.marketing_address_info import (
    generate_marketing_address_info
)


def test_generate_marketing_address_info():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    df_one = spark.createDataFrame(
        [
            (1, "Marketing", 100, 80),
            (2, "IT", 100, 90)
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
            (
                1,
                "John",
                "Lindehof 5, 4133 HB, Nederhemert",
                1000.0
            ),
            (
                2,
                "Mary",
                "2588 VD, Kropswolde",
                2000.0
            )
        ],
        [
            "id",
            "name",
            "address",
            "sales_amount"
        ]
    )

    actual_df = generate_marketing_address_info(
        df_one,
        df_two
    )

    expected_df = spark.createDataFrame(
        [
            (
                "Lindehof 5, 4133 HB, Nederhemert",
                "4133 HB"
            )
        ],
        [
            "address",
            "zipcode"
        ]
    )

    assert_df_equality(
        actual_df,
        expected_df
    )

    spark.stop()