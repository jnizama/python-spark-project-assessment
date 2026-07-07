"""
run_manager.py

Manages the daily execution folders.

Responsibilities:
- Create archive folder for daily executions.
- Copy incoming datasets to the archive folder.
- Create output folder for today's execution.
- Return all paths required by the application.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass
from datetime import date
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class RunPaths:
    """
    Stores all paths used during one execution.
    """

    dataset_one: str
    dataset_two: str
    dataset_three: str
    output_folder: str
    run_date: str


def prepare_daily_run() -> RunPaths:
    """
    Prepare today's execution.

    Folder structure created:

    data/
        incoming/
            dataset_one.csv
            dataset_two.csv
            dataset_three.csv

        archive/
            YYYY-MM-DD/
                dataset_one.csv
                dataset_two.csv
                dataset_three.csv

    output/
        YYYY-MM-DD/

    Returns
    -------
    RunPaths
        Object containing all paths required by the application.
    """

    run_date = date.today().strftime("%Y-%m-%d")

    project_root = Path(__file__).resolve().parent.parent

    #incoming_folder = project_root / "data" / "incoming"
    source_folder = project_root / "data"
    archive_folder = project_root / "data" / "archive" / run_date
    output_folder = project_root / "output" / run_date

    archive_folder.mkdir(parents=True, exist_ok=True)
    output_folder.mkdir(parents=True, exist_ok=True)

    datasets = [
        "dataset_one.csv",
        "dataset_two.csv",
        "dataset_three.csv",
    ]

    for filename in datasets:

        source = source_folder / filename
        destination = archive_folder / filename

        if not source.exists():
            raise FileNotFoundError(
                f"Incoming dataset not found: {source}"
            )

        if destination.exists():
            logger.info(
                "%s already archived for %s.",
                filename,
                run_date,
            )
        else:
            shutil.copy2(source, destination)
            logger.info(
                "%s archived successfully.",
                filename,
            )

    logger.info(
        "Daily run prepared successfully (%s).",
        run_date,
    )

    return RunPaths(
        dataset_one=str(archive_folder / "dataset_one.csv"),
        dataset_two=str(archive_folder / "dataset_two.csv"),
        dataset_three=str(archive_folder / "dataset_three.csv"),
        output_folder=str(output_folder),
        run_date=run_date,
    )