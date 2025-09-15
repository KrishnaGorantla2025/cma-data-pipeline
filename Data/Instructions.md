# CMA DaTA Unit technical test - Data Engineer (ETL)

## Preamble

### General

This exercise is designed to assess your ability to build a simple data pipeline in Python and to reason about how you would productionise it in the cloud.  
Some parts are deliberately open-ended. We are more interested in your approach and reasoning than in completeness.

### Timings

You should aim to spend **around 2 hours** on the coding task and preparing the short architecture slide(s).  
Please do not spend more than the allocated time.  
**You must submit your completed test within your allocated 12-hour slot.**
You may submit early if you wish.

### Your code

Your solution should be provided as a script (`ingest.py`) together with any supporting files.  
Please ensure we can run your code locally with Python. Dependencies should be listed in a `requirements.txt` file.  
Outputs should be written to an `out/` directory.

### The assessment

You will be assessed on:

* your general understanding and approach
* the quality and clarity of your code
* the appropriateness of your proposed architecture

There is no single "right" answer.  
Different sensible approaches can all receive full credit.

---

## The Assessment

### 1. Data & ETL

You are provided with two CSV files:

* `listings.csv` — marketplace item listings  
* `category_lookup.csv` — reference mapping of categories to segments  

#### Requirements

Write a Python script `ingest.py` that:

1. Reads both CSVs.  
2. Validates and cleans the data:  
   * The dataset must contain the following column: `date, seller_id, region, category, item_id, price_gbp`  
   * Drop rows with missing required fields or `price_gbp <= 0`  
   * Parse `date` (YYYY-MM-DD) and coerce `price_gbp` to numeric  
3. Deduplicates on `(date, item_id)` (keep the last row by file order).  
4. Enriches by left-joining to `category_lookup.csv` to add a `segment` column (i.e. keep all rows from listings.csv and, where category matches in the lookup, add the corresponding segment; otherwise leave segment blank).  
5. Writes the cleaned dataset as a **Parquet file** (`out/processed/data.parquet`) using Snappy compression.  
6. Writes a simple data quality report (`out/dq_report.json`) containing:  
   * source_rows – the total number of rows read from the raw listings.csv.
   * valid_rows_after_clean – the number of rows that remain after dropping any with missing required fields or invalid prices.
   * rows_after_dedup – the number of rows that remain after also removing duplicate (date, item_id) records (keeping the last occurrence).
   * dropped_non_positive_price – the count of rows removed because their price_gbp was <= 0 (or not numeric).
   * dropped_missing_required – the count of rows removed because one or more of the required fields was missing or empty.

#### Interfaces and Quality

* Script must be runnable via CLI:  
  ```bash
  python ingest.py --listings data/listings.csv --lookup data/category_lookup.csv --out out/
  ```
* Add logging.  
* Use type hints and docstrings where appropriate.  

---

### 2. Infrastructure Design

Prepare a short slide deck (1–2 slides) describing how you would productionise this pipeline in **AWS**.  
Consider:  

* Storage (S3 buckets, Parquet format)  
* Orchestration (EventBridge, Step Functions, Lambda/Glue)  
* Security and monitoring (IAM, KMS, CloudWatch)  
* Scaling and cost  

> **Important:** You will only be invited to present these slides if you are successful in passing the coding challenge. At interview, you will be asked to walk through your slides in a 10-minute presentation.

---

## Submission

Please package your solution as a `.zip` or Git repository containing:

* `ingest.py` and `requirements.txt`  
* Your short architecture slide(s)

---
