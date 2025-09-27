# Retail Data Pipeline (Python, ETL)

## Overview
This project demonstrates an end-to-end ETL (Extract, Transform, Load) process using Python and Pandas.  
The goal was to transform raw retail sales data into a clean, structured dataset with aggregated insights.

## Features
- Extracts raw CSV data
- Transforms dataset by:
  - Filtering weekly sales > 10,000
  - Keeping 7 relevant columns
  - Creating a `month` column from an existing `date` column, then dropping the raw date
  - Calculating average weekly sales per month, rounded to 2 decimals
- Loads the final dataset into a clean CSV file for downstream analysis

## Tech Stack
- Python (pandas)
- ETL pipeline design

## Files
- `pipeline.py` – ETL pipeline script
- `data/` – sample input and output datasets
- `requirements.txt` – Python dependencies

## How to Run
```bash
git clone https://github.com/nathanjbaron-DE/retail-data-pipeline.git

python3 -m venv venv
source venv/bin/activate

pip install -r requirements.txt
python pipeline.py

