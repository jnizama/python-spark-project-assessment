from pyspark.sql.functions import (
    col,
    round,
    row_number
)
from pyspark.sql.window import Window


def generate_top_3_performers(df_one, df_two):

    joined_df = (
        df_one
        .join(df_two, on="id", how="inner")
        .withColumn(
            "performance_percentage",
            round(
                (
                    col("calls_successful")
                    / col("calls_made")
                ) * 100,
                2
            )
        )
        .filter(
            col("performance_percentage") > 75
        )
    )

    window_spec = (
        Window
        .partitionBy("area")
        .orderBy(col("sales_amount").desc())
    )

    return (
        joined_df
        .withColumn(
            "rank",
            row_number().over(window_spec)
        )
        .filter(col("rank") <= 3)
        .select(
            "area",
            "name",
            "sales_amount",
            "performance_percentage",
            "rank"
        )
        .orderBy(
            "area",
            "rank"
        )
    )