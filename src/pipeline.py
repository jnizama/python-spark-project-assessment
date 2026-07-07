"""
pipeline.py

Main pipeline responsible for orchestrating the complete
sales data processing workflow
"""

import logging

from pyspark.sql import SparkSession

from data_quality import run_basic_data_quality_checks
from it_data import generate_it_data
from marketing_address_info import generate_marketing_address_info
from department_breakdown import generate_department_breakdown
from top_3 import generate_top_3_performers
from top_3_products_netherlands import (generate_top_3_products_netherlands)
from best_salesperson import (generate_best_salesperson_per_country)
from run_manager import prepare_daily_run
logger = logging.getLogger(__name__)


class SalesPipeline:
    """
    Main class responsible for executing
    the complete ETL pipeline.
    """

    def __init__(self):

        #self.run = prepare_daily_run()
        self.run_paths = prepare_daily_run()
        logger.info(
            "Run date: %s",
            self.run_paths.run_date
        )
        self.spark = (
            SparkSession.builder
            .appName("sales-data")
            .master("local[*]")
            .getOrCreate()
        )
        self.df_one = None
        self.df_two = None
        self.df_three = None

    def load_csv(self, path: str):

        return (
            self.spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(path)
        )

    def load_data(self):

        logger.info("Loading archived datasets...")

        self.df_one = self.load_csv(
            self.run_paths.dataset_one
        )
        self.df_two = self.load_csv(
            self.run_paths.dataset_two
        )
        self.df_three = self.load_csv(
            self.run_paths.dataset_three
        )

    @staticmethod
    def save_output(df, output_path: str):

        (
            df
            .coalesce(1)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(output_path)
        )

    def run(self):

        self.load_data()
        run_basic_data_quality_checks(
            self.df_one,
            self.df_two,
            self.df_three
        )
        #
        # Task 1
        #
        it_df = generate_it_data(
            self.df_one,
            self.df_two
        )
        self.save_output(
            it_df,
            f"{self.run_paths.output_folder}/it_data"
        )
        logger.info("IT Data generated.")

        #
        # Task 2
        #

        marketing_df = (
            generate_marketing_address_info(
                self.df_one,
                self.df_two
            )
        )

        self.save_output(
            marketing_df,
            f"{self.run_paths.output_folder}/marketing_address_info"
        )
        logger.info("Marketing Address generated.")

        #
        # Task 3
        #

        department_df = (
            generate_department_breakdown(
                self.df_one,
                self.df_two
            )
        )

        self.save_output(
            department_df,
            f"{self.run_paths.output_folder}/department_breakdown"
        )

        logger.info("Department Breakdown generated.")

        #
        # Task 4
        #
        top3_df = (
            generate_top_3_performers(
                self.df_one,
                self.df_two
            )
        )

        self.save_output(
            top3_df,
            f"{self.run_paths.output_folder}/top_3"
        )
        logger.info("Top 3 generated.")

        #
        # Task 5
        #

        top_products_df = (
            generate_top_3_products_netherlands(
                self.df_one,
                self.df_three
            )
        )
        self.save_output(
            top_products_df,
            f"{self.run_paths.output_folder}/top_3_most_sold_per_department_netherlands"
        )

        logger.info("Top Products generated.")

        #
        # Task 6
        #

        best_df = (
            generate_best_salesperson_per_country(
                self.df_two,
                self.df_three
            )
        )
        self.save_output(
            best_df,
            f"{self.run_paths.output_folder}/best_salesperson"
        )
        logger.info("Best Salesperson generated.")

    def close(self):
        logger.info("Stopping Spark session...")
        self.spark.stop()