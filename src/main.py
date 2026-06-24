import argparse
import logging
from pyspark.sql import SparkSession
from data_quality import run_basic_data_quality_checks
from it_data import generate_it_data

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

#for task 2
from marketing_address_info import generate_marketing_address_info
#for task 3
from department_breakdown import generate_department_breakdown
#for task 4
from top_3 import generate_top_3_performers
#for task 5
from top_3_products_netherlands import (
    generate_top_3_products_netherlands
)
# for task 6
from best_salesperson import (
    generate_best_salesperson_per_country
)

def create_spark_session() -> SparkSession:
    """
    Create and return a Spark session.
    """
    return (
        SparkSession.builder
        .appName("sales-data")
        .master("local[*]")
        .getOrCreate()
    )


def load_csv(spark: SparkSession, path: str):
    """
    Load a CSV file into a DataFrame.
    """
    return (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(path)
    )


def main() -> None:

    parser = argparse.ArgumentParser(
        description="Sales Data Processing Application"
    )

    parser.add_argument(
        "--dataset-one",
        required=True,
        help="Path to dataset_one.csv"
    )

    parser.add_argument(
        "--dataset-two",
        required=True,
        help="Path to dataset_two.csv"
    )

    parser.add_argument(
        "--dataset-three",
        required=True,
        help="Path to dataset_three.csv"
    )

    args = parser.parse_args()

    spark = create_spark_session()

    # Load datasets
    df_one = load_csv(spark, args.dataset_one)
    df_two = load_csv(spark, args.dataset_two)
    df_three = load_csv(spark, args.dataset_three)

    # Data Quality Checks
    run_basic_data_quality_checks(
        df_one,
        df_two,
        df_three
    )

    # Task 1
    it_df = generate_it_data(
        df_one,
        df_two
    )

    (
        it_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("output/it_data")
    )

    #print("\nIT Data output generated successfully.")
    #print("Location: output/it_data")
    logger.info("IT Data output generated successfully.")
    logger.info("Location: output/it_data")

    # Task 2

    marketing_df = generate_marketing_address_info(
        df_one,
        df_two
    )

    (
        marketing_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("output/marketing_address_info")
    )

    #print("\nMarketing Address Info output generated successfully.")
    #print("Location: output/marketing_address_info")
    logger.info(
        "Marketing Address Info output generated successfully."
    )
    logger.info(
        "Location: output/marketing_address_info"
    )

    # Task 3

    department_df = generate_department_breakdown(
        df_one,
        df_two
    )

    (
        department_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("output/department_breakdown")
    )

    #print("\nDepartment Breakdown output generated successfully.")
    #print("Location: output/department_breakdown")
    logger.info(
        "Department Breakdown output generated successfully"
    )
    logger.info(
        "Location: output/department_breakdown"
    )

    # Task 4

    top_3_df = generate_top_3_performers(
        df_one,
        df_two
    )

    (
        top_3_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("output/top_3")
    )

    print("\nTop 3 output generated successfully.")
    
    #Task 5

    top_products_df = (
        generate_top_3_products_netherlands(
            df_one,
            df_three
        )
    )

    (
        top_products_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv(
            "output/top_3_most_sold_per_department_netherlands"
        )
    )

    #print("\nTop 3 Most Sold Products output generated successfully")
    logger.info("Top 3 Most Sold Products output generated successfully")
    logger.info("Location: output/department_breakdown")

    # Taks 6

    best_salesperson_df = (
        generate_best_salesperson_per_country(
            df_two,
            df_three
        )
    )

    (
        best_salesperson_df
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("header", True)
        .csv("output/best_salesperson")
    )

    #print("\nBest Salesperson output generated successfully.")
    logger.info("Best Salesperson output generated successfully")    
    # closing the spart instance
    
    spark.stop()


if __name__ == "__main__":
    main()