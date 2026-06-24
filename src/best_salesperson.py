from pyspark.sql.functions import (
    col,
    row_number,
    max
)
from pyspark.sql.window import Window


def generate_best_salesperson_per_country(
    df_two,
    df_three
):

    country_salespeople = (
        df_three
        .select(
            "country",
            "caller_id"
        )
        .distinct()
        .join(
            df_two,
            df_three.caller_id == df_two.id,
            "inner"
        )
    )

    window_spec = (
        Window
        .partitionBy("country")
        .orderBy(
            col("sales_amount").desc()
        )
    )

    return (
        country_salespeople
        .withColumn(
            "rank",
            row_number().over(window_spec)
        )
        .filter(
            col("rank") == 1
        )
        .select(
            "country",
            "id",
            "name",
            "sales_amount"
        )
        .orderBy("country")
    )