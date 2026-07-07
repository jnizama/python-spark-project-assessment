from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

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

from src.top_3 import generate_top_3_performers


def test_generate_top_3_performers():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    df_one = spark.createDataFrame(
        [
            (1, "IT", 100, 90),
            (2, "IT", 100, 80),
            (3, "IT", 100, 70),
            (4, "IT", 100, 60),
            (5, "HR", 100, 90),
            (6, "HR", 100, 80),
            (7, "HR", 100, 70),
            (8, "HR", 100, 60),
        ],
        [
            "id",
            "area",
            "calls_made",
            "calls_successful",
        ],
    )

    df_two = spark.createDataFrame(
        [
            (1, "John", "Addr1", 5000.0),
            (2, "Mary", "Addr2", 4000.0),
            (3, "Peter", "Addr3", 3000.0),
            (4, "James", "Addr4", 2000.0),
            (5, "Ana", "Addr5", 6000.0),
            (6, "Sara", "Addr6", 5000.0),
            (7, "Julia", "Addr7", 4000.0),
            (8, "Laura", "Addr8", 1000.0),
        ],
        [
            "id",
            "name",
            "address",
            "sales_amount",
        ],
    )

    actual_df = generate_top_3_performers(
        df_one,
        df_two
    )

    expected_schema = StructType([
        StructField("area", StringType(), True),
        StructField("name", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("performance_percentage", DoubleType(), True),
        StructField("rank", IntegerType(), False),
    ])

    expected_df = spark.createDataFrame(
        [
            ("HR", "Ana", 6000.0, 90.0, 1),
            ("HR", "Sara", 5000.0, 80.0, 2),
            ("IT", "John", 5000.0, 90.0, 1),
            ("IT", "Mary", 4000.0, 80.0, 2),
        ],
        schema=expected_schema,
    )

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_nullable=True,
    )

    #
    # Functional requirement:
    # Top 3 performers per department
    #
    assert actual_df.filter(col("rank") > 3).count() == 0

    spark.stop()