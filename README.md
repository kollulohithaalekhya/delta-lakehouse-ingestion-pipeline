# Delta Lakehouse Ingestion Pipeline

## Overview

This project implements a production-ready Data Lakehouse ingestion pipeline using Delta Lake. It processes GitHub Archive data and demonstrates incremental ingestion, schema evolution, data validation, and time travel recovery.

The pipeline follows a Medallion Architecture and includes a backend API and a frontend UI for querying and exploring the data.

---

## Architecture

Bronze Layer
Stores raw validated data

Silver Layer
Cleans data, removes duplicates, standardizes timestamps

Corrected Layer
Restores clean data using Delta Lake time travel

---

## Tech Stack

Python
Delta Lake (delta-rs)
Pandas
Pydantic
DuckDB
Flask

---

## Project Structure

delta-lakehouse-ingestion-pipeline/

data/
source/
lakehouse/
bronze/
silver/
silver_corrected/

pipeline/
ingest.py
models.py
correct_data.py

explorer/
api/app.py
ui/index.html

docs/
transaction_log_analysis.md

Dockerfile
docker-compose.yml
requirements.txt
README.md

---

## Full Execution Steps

Follow all steps in order

---

### Step 1 Clone Repository

git clone https://github.com/your-username/delta-lakehouse-ingestion-pipeline.git
cd delta-lakehouse-ingestion-pipeline

---

### Step 2 Create Virtual Environment

Windows PowerShell

python -m venv venv
venv\Scripts\activate

Git Bash

python -m venv venv
source venv/Scripts/activate

---

### Step 3 Install Dependencies

pip install -r requirements.txt

If flask cors error occurs

pip install flask-cors

---

### Step 4 Create Required Folders

mkdir -p data/source
mkdir -p data/lakehouse

---

### Step 5 Download Dataset

curl -k -O https://data.gharchive.org/2015-01-01-15.json.gz
curl -k -O https://data.gharchive.org/2015-01-01-16.json.gz
curl -k -O https://data.gharchive.org/2015-01-01-17.json.gz
curl -k -O https://data.gharchive.org/2015-01-01-19.json.gz

---

### Step 6 Move Files

mv 2015-01-01-15.json.gz data/source/day_1.json.gz
mv 2015-01-01-16.json.gz data/source/day_2.json.gz
mv 2015-01-01-17.json.gz data/source/day_3.json.gz
mv 2015-01-01-19.json.gz data/source/day_5.json.gz

---

### Step 7 Run Data Pipeline

python pipeline/ingest.py --day 1
python pipeline/ingest.py --day 2
python pipeline/ingest.py --day 3
python pipeline/ingest.py --day 5

---

### Step 8 Verify Data Creation

ls data/lakehouse

Expected output

bronze
silver
silver_corrected

---

### Step 9 Validate Data Using DuckDB

python

import duckdb

con = duckdb.connect()
con.execute("INSTALL delta")
con.execute("LOAD delta")

result = con.execute("""
SELECT COUNT(*) FROM (
SELECT id, COUNT(*) c
FROM delta_scan('data/lakehouse/silver')
GROUP BY id HAVING c > 1
)
""").fetchall()

print(result)

Expected output

[(0,)]

---

### Step 10 Time Travel Recovery

python pipeline/correct_data.py

---

### Step 11 Verify Corrected Data

SELECT COUNT(*)
FROM delta_scan('data/lakehouse/silver_corrected')
WHERE id IS NULL;

Expected output

0

---

### Step 12 Run Backend API

python explorer/api/app.py

Test in browser

http://localhost:5000/api/health

---

### Step 13 API Endpoints

GET /api/health
GET /api/tables
GET /api/preview/{table}
POST /api/query

Example request

{
"query": "SELECT * FROM delta_scan('data/lakehouse/silver') LIMIT 10"
}

---

### Step 14 Run UI

Open file directly

explorer/ui/index.html

Or use Live Server

http://127.0.0.1:5500/explorer/ui/index.html

---

### Step 15 Use UI

Click Bronze Silver Corrected buttons
Run SQL queries
View results

---

## Docker Usage

docker-compose up --build

Access API

http://localhost:5000/api/health

---

## Common Issues

flask_cors not found
Install using pip install flask-cors

API not connecting
Ensure backend is running

400 error
Run pipeline again

SSL error
Use http instead of https

---
