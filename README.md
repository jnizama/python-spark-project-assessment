# Sales Data Processing Application

## Overview

This project was built with Python 3.10 and PySpark 3.5.0.

The program reads three CSV files with employee, sales, and call data from a telemarketing company. It checks that the data is valid and creates the required output files.

## Features

### Data Quality Checks

The application following checks before processing:

* ID fields are non-null
* ID fields are unique
* Row counts match expected values
* Numeric fields are not negative

### Result 1

Generates a dataset containing:

* Employees from the IT department
* Joined information from dataset_one and dataset_two
* Ordered by sales amount (descending)
* Top 100 records only

Output directory:

```text
output/it_data
```

### Result 2

Generates a dataset containing:

* Marketing department employees only
* Address information
* Extracted zipcode in a separate column

Output directory:

```text
output/marketing_address_info
```

### Result 3

Generates aggregated information per department:

* Total sales amount
* Success percentage based on calls_successful / calls_made

Output directory:

```text
output/department_breakdown
```

## Project Structure Path

```text
TCS_ASSESSMENT
├── data
│   ├── dataset_one.csv
│   ├── dataset_two.csv
│   └── dataset_three.csv
├── output
├── src
│   ├── main.py
│   ├── data_quality.py
│   ├── it_data.py
│   ├── marketing_address_info.py
│   └── department_breakdown.py
├── tests
├── requirements.txt
└── README.md
```

## Installation

Create a virtual environment:

```bash
python3.10 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

## Running the Application

Execute:

```bash
python src/main.py \
  --dataset-one data/dataset_one.csv \
  --dataset-two data/dataset_two.csv \
  --dataset-three data/dataset_three.csv
```

Generated outputs will be stored in the output directory.

## Technologies

* Python 3.10
* PySpark 3.5.0
* Pytest
* Chispa

## Future Improvements

* Intermediate Data Quality Checks
* Bonus Outputs (#4, #5, #6)
* Logging framework
* GitHub Actions CI/CD pipeline
* Additional automated tests
