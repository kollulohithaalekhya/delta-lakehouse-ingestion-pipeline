from deltalake import DeltaTable, write_deltalake

SILVER_PATH = "data/lakehouse/silver"
CORRECTED_PATH = "data/lakehouse/silver_corrected"


def restore_previous_version():
    dt = DeltaTable(SILVER_PATH)

    current_version = dt.version()
    print("Current version:", current_version)

    # ✅ Correct way to time travel
    old_dt = DeltaTable(SILVER_PATH, version=current_version - 1)
    df = old_dt.to_pandas()
    df = df[df["id"].notna()]
    write_deltalake(
        CORRECTED_PATH,
        df,
        mode="overwrite"
    )

    print("Data restored successfully!")


if __name__ == "__main__":
    restore_previous_version()