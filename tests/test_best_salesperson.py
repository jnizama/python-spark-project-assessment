from pyspark.sql import SparkSession
from chispa import assert_df_equality

import sys
import os
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    LongType,
    DoubleType,
)

sys.path.append(
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            ".."
        )
    )
)

from src.best_salesperson import (
    generate_best_salesperson_per_country
)


def test_generate_best_salesperson_per_country():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    df_two = spark.createDataFrame(
        [
            (1, "John", "Addr1", 1000.0),
            (2, "Mary", "Addr2", 1000.0),
            (3, "Peter", "Addr3", 1000.0),
        ],
        [
            "id",
            "name",
            "address",
            "sales_amount"
        ]
    )

    df_three = spark.createDataFrame(
        [
            (1, 1, "A", "X", 30, "Belgium", "Laptop", 20),
            (2, 2, "B", "X", 30, "Belgium", "Laptop", 10),
            (3, 3, "C", "X", 30, "Netherlands", "Laptop", 30),
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

    actual_df = generate_best_salesperson_per_country(
        df_two,
        df_three
    )

    expected_schema = StructType([
    StructField("country", StringType(), True),
    StructField("id", LongType(), True),
    StructField("name", StringType(), True),
    StructField("sales_amount", DoubleType(), True),
])

    expected_df = spark.createDataFrame(
        [
            ("Belgium", 1, "John", 1000.0),
            ("Netherlands", 3, "Peter", 1000.0),
        ],
        schema=expected_schema,
    )

    print("\n===== SCHEMA =====")
    actual_df.printSchema()

    print("\n===== DATA =====")
    actual_df.show(truncate=False)

    assert_df_equality(
        actual_df,
        expected_df,
        ignore_row_order=False,
        ignore_nullable=True,
    )

    assert (
        actual_df
        .select("country")
        .distinct()
        .count()
        ==
        actual_df.count()
    )
    spark.stop()