import argparse
import gzip
import json
import pandas as pd
import random

from deltalake import write_deltalake
from models import Event
from deltalake import DeltaTable

BRONZE_PATH = "data/lakehouse/bronze"
SILVER_PATH = "data/lakehouse/silver"


def read_json_gz(file_path):
    records = []
    with gzip.open(file_path, 'rt', encoding='utf-8', errors='ignore') as f:
        for line in f:
            try:
                data = json.loads(line)
                event = Event(**data)   # validation
                records.append(event.model_dump())
            except Exception as e:
                print("Skipped invalid record:", e)

    return pd.DataFrame(records)


def write_bronze(df):
    # 🔥 Fix 1: Drop columns that are fully NULL
    df = df.dropna(axis=1, how='all')

    # 🔥 Fix 2: Convert nested objects (actor, repo) → JSON strings
    df["actor"] = df["actor"].astype(str)
    df["repo"] = df["repo"].astype(str)

    write_deltalake(
        BRONZE_PATH,
        df,
        mode="append",
        schema_mode="merge"   # 🔥 REQUIRED
    )

def transform_to_silver():
    print("Transforming to Silver layer...")

    try:
        dt = DeltaTable(BRONZE_PATH)
        df = dt.to_pandas()
    except Exception as e:
        print("Error reading Bronze:", e)
        return

    # ✅ Deduplication (IMPORTANT)
    df = df.drop_duplicates(subset=["id"], keep="last")

    # ✅ Fix timestamp
    df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    # ✅ Write Silver
    write_deltalake(
        SILVER_PATH,
        df,
        mode="overwrite",
        schema_mode="merge"   # 🔥 IMPORTANT FIX
    )

    print("Silver table created!")

def add_schema_evolution(df, day):
    if day == 3:
        print("Applying schema evolution (Day 3)...")

        # Add new column
        df["device_fingerprint"] = [
            f"device_{random.randint(1000,9999)}" for _ in range(len(df))
        ]

    return df

def inject_bad_data(df, day):
    if day == 5:
        print("Injecting bad data (Day 5)...")

        # Corrupt some rows
        df.loc[:5, "id"] = None   # invalid IDs
        df.loc[:5, "created_at"] = "INVALID_DATE"

    return df

def main(day):
    file_path = f"data/source/day_{day}.json.gz"

    print(f"Processing Day {day}...")
    df = read_json_gz(file_path)

    # 🔥 Apply schema evolution
    df = add_schema_evolution(df, day)
    df = inject_bad_data(df, day)
    print("Valid records:", len(df))

    write_bronze(df)

    print("Bronze ingestion complete!")

    # 🔥 NEW STEP
    transform_to_silver()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", type=int, required=True)

    args = parser.parse_args()

    main(args.day)