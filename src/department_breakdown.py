# to the amount of money and percentage should be easily readable.
from pyspark.sql.functions import format_number

from pyspark.sql.functions import (
    col,
    sum,
    round
)


def generate_department_breakdown(df_one, df_two):

    joined_df = (
        df_one
        .join(df_two, on="id", how="inner")
    )

    return (
        joined_df
        .groupBy("area")
        .agg(
            format_number(
                sum("sales_amount"),
                2
            ).alias("total_sales_amount"),

            round(
                (
                    sum("calls_successful")
                    / sum("calls_made")
                ) * 100,
                2
            ).alias("success_percentage")
        )
        .orderBy(col("total_sales_amount").desc())
    )