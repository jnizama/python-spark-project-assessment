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

from src.top_3_products_netherlands import (
    generate_top_3_products_netherlands
)


def test_generate_top_3_products_netherlands():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    df_one = spark.createDataFrame(
        [
            (1, "IT", 0, 0),
            (2, "IT", 0, 0),
            (3, "Marketing", 0, 0),
        ],
        [
            "id",
            "area",
            "calls_made",
            "calls_successful"
        ]
    )

    df_three = spark.createDataFrame(
        [
            (1, 1, "ABC", "X", 40, "Netherlands", "Laptop", 20),
            (2, 1, "ABC", "X", 40, "Netherlands", "Mouse", 15),
            (3, 1, "ABC", "X", 40, "Netherlands", "Keyboard", 10),
            (4, 1, "ABC", "X", 40, "Netherlands", "Screen", 5),
        ],
        [
            "id",
            "caller_id",
            "company",
            "recipient",
            "age",
            "country",
            "product_sold",
            "quantity"
        ]
    )

    actual_df = generate_top_3_products_netherlands(
        df_one,
        df_three
    )

    from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    IntegerType,
)


    print("\n===== SCHEMA =====")
    actual_df.printSchema()

    print("\n===== DATA =====")
    actual_df.show(truncate=False)

    expected_schema = StructType([
        StructField("area", StringType(), True),
        StructField("product_sold", StringType(), True),
        StructField("total_quantity", LongType(), True),
        StructField("rank", IntegerType(), False),
    ])

    expected_df = spark.createDataFrame(
        [
            ("IT", "Laptop", 20, 1),
            ("IT", "Mouse", 15, 2),
            ("IT", "Keyboard", 10, 3),
        ],
        schema=expected_schema,
    )
    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_nullable=True,
    )

    from pyspark.sql.functions import col

    assert (
        actual_df
        .filter(col("rank") > 3)
        .count()
        == 0
    )
    spark.stop()