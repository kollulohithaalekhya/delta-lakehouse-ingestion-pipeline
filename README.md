# Delta Lakehouse Ingestion Pipeline

## Overview

This project implements a production-style Data Lakehouse ingestion pipeline using Delta Lake. It processes GitHub Archive data and demonstrates incremental ingestion, schema evolution, data validation, and time travel recovery.

The pipeline follows a Medallion Architecture with Bronze and Silver layers, ensuring raw data ingestion and refined data processing.

---

## Architecture

### Bronze Layer

* Stores raw ingested data
* Data is validated using Pydantic
* Written as Delta tables

### Silver Layer

* Cleans and transforms data
* Removes duplicates
* Standardizes timestamps
* Stores refined data in Delta format

---

## Tech Stack

* Python
* Delta Lake (delta-rs)
* Pandas
* Pydantic
* DuckDB
* Flask (API layer)

---

## Project Structure

```
delta-lakehouse-ingestion-pipeline/
│
├── data/
│   ├── source/
│   └── lakehouse/
│       ├── bronze/
│       ├── silver/
│       └── silver_corrected/
│
├── pipeline/
│   ├── ingest.py
│   ├── models.py
│   └── correct_data.py
│
├── explorer/
│   ├── api/
│   │   └── app.py
│   └── ui/
│       └── index.html
│
├── docs/
│   └── transaction_log_analysis.md
│
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── README.md
```

---

## Features

### Data Ingestion

* Reads compressed JSON files (.json.gz)
* Validates records using Pydantic models
* Writes validated data to Bronze Delta table

### Data Transformation

* Removes duplicate records using unique ID
* Converts timestamps to standard format
* Writes cleaned data to Silver table

### Incremental Processing

* Supports ingestion across multiple days
* Appends new data without overwriting previous data

### Schema Evolution

* Handles addition of new columns dynamically
* Uses schema merge to update Delta tables
* Demonstrated using device_fingerprint field

### Bad Data Handling

* Simulates corrupted data input
* Ensures pipeline continues without failure
* Invalid records are handled safely

### Time Travel and Recovery

* Uses Delta Lake versioning
* Restores previous clean version of data
* Writes recovered data to a separate table

---

## Setup Instructions

### 1. Clone the repository

```
git clone https://github.com/<your-username>/delta-lakehouse-ingestion-pipeline.git
cd delta-lakehouse-ingestion-pipeline
```

### 2. Create virtual environment

```
python -m venv venv
venv\Scripts\activate
```

### 3. Install dependencies

```
pip install -r requirements.txt
```

---

## Data Preparation

Download sample data from GitHub Archive:

```
curl -k -O https://data.gharchive.org/2015-01-01-15.json.gz
curl -k -O https://data.gharchive.org/2015-01-01-16.json.gz
curl -k -O https://data.gharchive.org/2015-01-01-17.json.gz
curl -k -O https://data.gharchive.org/2015-01-01-19.json.gz
```

Rename and move files:

```
mv 2015-01-01-15.json.gz data/source/day_1.json.gz
mv 2015-01-01-16.json.gz data/source/day_2.json.gz
mv 2015-01-01-17.json.gz data/source/day_3.json.gz
mv 2015-01-01-19.json.gz data/source/day_5.json.gz
```

---

## Running the Pipeline

### Run ingestion for each day

```
python pipeline/ingest.py --day 1
python pipeline/ingest.py --day 2
python pipeline/ingest.py --day 3
python pipeline/ingest.py --day 5
```

---

## Data Validation

Use DuckDB to verify results:

```
import duckdb

con = duckdb.connect()
con.execute("LOAD delta")

result = con.execute("""
SELECT COUNT(*) FROM (
  SELECT id, COUNT(*) c
  FROM delta_scan('data/lakehouse/silver')
  GROUP BY id HAVING c > 1
)
""").fetchall()

print(result)
```

Expected output:

```
[(0,)]
```

---

## Time Travel Recovery

Restore previous clean version:

```
python pipeline/correct_data.py
```

Verify corrected data:

```
SELECT COUNT(*) 
FROM delta_scan('data/lakehouse/silver_corrected')
WHERE id IS NULL;
```

Expected output:

```
0
```

---

## API Endpoints

Start the API:

```
python explorer/api/app.py
```

Available endpoints:

* GET /api/health
* GET /api/tables
* GET /api/preview/{table}
* POST /api/query

Example query request:

```
{
  "query": "SELECT * FROM delta_scan('data/lakehouse/silver') LIMIT 10"
}
```

---

## UI

Open:

```
explorer/ui/index.html
```

Features:

* Table preview (Bronze, Silver, Corrected)
* SQL query execution
* Dynamic results table

---

## Docker Usage

Build and run:

```
docker-compose up --build
```

Access API:

```
http://localhost:5000/api/health
```

---

## Delta Lake Internals

Delta Lake maintains a transaction log in the `_delta_log` directory. Each operation creates a new version, enabling:

* Data versioning
* Schema tracking
* Time travel queries
* Reliable data recovery

---

## Key Learnings

* Building a Medallion Architecture pipeline
* Handling schema evolution in Delta Lake
* Implementing incremental data ingestion
* Managing corrupted data in pipelines
* Using time travel for data recovery
* Querying Delta tables with DuckDB
* Designing API and UI for data exploration

---

## Conclusion

This project demonstrates how to design and implement a robust Data Lakehouse ingestion pipeline using modern data engineering tools. It highlights key concepts such as schema evolution, incremental processing, and data recovery, making it suitable for real-world applications.
