from pyspark.sql.functions import col
from src.core.decorators import log_execution

@log_execution
def generate_it_data(df_one, df_two):
    return (
        df_one
        .join(df_two, on="id", how="inner")
        .filter(col("area") == "IT")
        .orderBy(col("sales_amount").desc())
        .limit(100)
    )