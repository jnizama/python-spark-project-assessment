# Sales Data Processing Application

## Overview

This project was developed using **Python 3.10** and **PySpark 3.5.0**.

The application processes three datasets containing employee, sales and telemarketing information. Before generating the required business outputs, it performs data quality validations to ensure that the data is suitable for analysis.

The solution was designed following a simple ETL architecture using object-oriented principles, separating orchestration, business logic and infrastructure.

---

# Features

## Data Quality

Before processing the datasets, the application validates:

- ID fields are not null.
- ID fields are unique.
- Expected row counts are correct.
- Numeric values are not negative.

Whenever a validation fails, a warning is written to the log.

---

# Generated Outputs

The application generates the following business outputs.

## Output #1 – IT Data

- Join Dataset 1 and Dataset 2
- IT department only
- Ordered by Sales Amount (descending)
- Top 100 employees

---

## Output #2 – Marketing Address Information

- Marketing department only
- Address information
- Zipcode extraction

---

## Output #3 – Department Breakdown

For every department:

- Total Sales Amount
- Success Percentage

---

## Output #4 – Top 3 Performers per Department

Returns the three best performing employees for every department.

---

## Output #5 – Top 3 Most Sold Products per Department (Netherlands)

Returns the three most sold products for every department considering only companies located in the Netherlands.

---

## Output #6 – Best Salesperson per Country

Returns the best overall salesperson for every country.

---

# Daily & Historical Execution

The application automatically manages historical executions.

The source datasets are always placed inside the **data** directory:

```text
data/
├── dataset_one.csv
├── dataset_two.csv
└── dataset_three.csv
```

When the application starts, it automatically:

1. Creates a folder using the current execution date.
2. Copies the original datasets into the historical archive.
3. Processes the archived datasets.
4. Stores all generated outputs in a dated output folder.

Example:

```text
data/
├── dataset_one.csv
├── dataset_two.csv
├── dataset_three.csv
└── archive/
    ├── 2026-07-06/
    ├── 2026-07-07/
    └── YYYY-MM-DD/
```

Outputs are also stored historically:

```text
output/
├── 2026-07-06/
├── 2026-07-07/
└── YYYY-MM-DD/
```

The folder name is generated automatically from the current system date.

This allows every execution to be reproducible while preserving historical input and output data.

> **Note**
>
> The dated folders included in this repository correspond to previous executions and are provided only as examples. Every new execution automatically creates a new folder using the current execution date.

---

# Project Structure

```text
TCS_Assessment
│
├── data
│   ├── dataset_one.csv
│   ├── dataset_two.csv
│   ├── dataset_three.csv
│   └── archive
│
├── output
│
├── src
│   ├── main.py
│   ├── pipeline.py
│   │
│   ├── core
│   │   ├── decorators.py
│   │   ├── run_manager.py
│   │   └── data_quality.py
│   │
│   └── ...
│
├── tests
│
├── requirements.txt
└── README.md
```

---

# Software Design

The application follows a simple object-oriented architecture.

- **SalesPipeline** orchestrates the complete execution.
- **RunManager** prepares daily and historical executions.
- Business logic is implemented in independent transformation modules.
- Logging decorators are used to trace execution of business functions.
- Responsibilities are clearly separated between orchestration, infrastructure and business logic.

---

# Installation

Create a virtual environment:

```bash
python3.10 -m venv .venv
```

Activate it:

```bash
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

---

# Running the Application

Simply execute:

```bash
python src/main.py
```

No command-line parameters are required.

The application automatically:

- Reads the datasets from the **data/** directory.
- Archives them into **data/archive/<current-date>/**.
- Processes the archived datasets.
- Generates all outputs into **output/<current-date>/**.

---

# Running the Tests

Run all tests:

```bash
python -m pytest
```

Run an individual test:

```bash
pytest tests/test_top_3.py
```

Expected result:

```text
=========================
6 passed
=========================
```

---

# Testing

The project includes both:

- Unit Tests
- Functional (Business) Tests

Tests were implemented using:

- pytest
- chispa

Unit tests validate the transformation logic.

Business tests validate that the generated outputs satisfy the functional requirements described in the assessment.

---

# Technologies

- Python 3.10
- PySpark 3.5.0
- Pytest
- Chispa

---

# Future Improvements

- GitHub Actions CI/CD
- Rotating log files
- Package distribution
- Additional business validations
