from pyspark.sql.functions import (
    col,
    sum,
    row_number
)
from pyspark.sql.window import Window


def generate_top_3_products_netherlands(
    df_one,
    df_three
):

    joined_df = (
        df_three
        .join(
            df_one,
            df_three.caller_id == df_one.id,
            "inner"
        )
        .filter(
            col("country") == "Netherlands"
        )
    )

    aggregated_df = (
        joined_df
        .groupBy(
            "area",
            "product_sold"
        )
        .agg(
            sum("quantity")
            .alias("total_quantity")
        )
    )

    window_spec = (
        Window
        .partitionBy("area")
        .orderBy(
            col("total_quantity").desc()
        )
    )

    return (
        aggregated_df
        .withColumn(
            "rank",
            row_number().over(window_spec)
        )
        .filter(col("rank") <= 3)
        .orderBy(
            "area",
            "rank"
        )
    )