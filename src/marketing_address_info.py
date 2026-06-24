from pyspark.sql.functions import col
from pyspark.sql.functions import regexp_extract


def generate_marketing_address_info(df_one, df_two):

    return (
        df_one
        .join(df_two, on="id", how="inner")
        .filter(col("area") == "Marketing")
        .select(
            "address",
            regexp_extract(
                col("address"),
                r"(\d{4}\s[A-Z]{2})",
                1
            ).alias("zipcode")
        )
    )