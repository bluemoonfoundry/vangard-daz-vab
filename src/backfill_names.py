# src/backfill_names.py
import json
import os
import pathlib
import sqlite3
import sys

from utilities import SQLITE_DB_PATH, DAZ_EXTRACTED_PRODUCT_FILE,DAZ_EXTRACTED_PRODUCT_TABLE
from dotenv import load_dotenv

load_dotenv()

# --- Configuration ---


def backfill_product_names():
    """
    Adds a 'name' column to the product table if it doesn't exist,
    then reads a JSON file and updates the new column with the 'title'
    for each matching SKU.
    """

    # --- Step 1: Add the new column to the database ---
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    try:
        print("Checking for 'name' column and adding it if it doesn't exist...")
        # This will safely add the column only if it's missing.
        cursor.execute(f"ALTER TABLE {DAZ_EXTRACTED_PRODUCT_TABLE} ADD COLUMN name TEXT")
        print("-> 'name' column added successfully.")
    except sqlite3.OperationalError as e:
        # This error is expected if the column already exists.
        if "duplicate column name" in str(e):
            print("-> 'name' column already exists. Skipping.")
        else:
            # Re-raise the error if it's something else
            raise e

    conn.commit()

    # --- Step 2: Read the source JSON file ---
    print(f"\nReading product name data from: {DAZ_EXTRACTED_PRODUCT_FILE}")
    try:
        # We assume the JSON file is in the parent directory of 'src'
        json_path = DAZ_EXTRACTED_PRODUCT_FILE
        with open(json_path, "r", encoding="utf-8") as f:
            products_data = json.load(f)

        # Create a fast lookup dictionary: {sku: title}
        name_map = {
            item["sku"]: item["title"]
            for item in products_data
            if "sku" in item and "title" in item
        }
        print(f"Found {len(name_map)} products with titles in the JSON file.")

    except FileNotFoundError:
        print(
            f"Error: Source file not found at '{DAZ_EXTRACTED_PRODUCT_FILE}'. Please make sure it exists.",
            file=sys.stderr,
        )
        conn.close()
        return
    except (json.JSONDecodeError, KeyError) as e:
        print(
            f"Error: Could not process '{DAZ_EXTRACTED_PRODUCT_FILE}'. Ensure it's a valid JSON array of objects with 'sku' and 'title' keys.",
            file=sys.stderr,
        )
        print(f"Details: {e}", file=sys.stderr)
        conn.close()
        return

    # --- Step 3: Update the database records ---
    print("\nStarting database update process...")

    # We can update many rows efficiently with executemany
    update_data = []
    for sku, name in name_map.items():
        update_data.append((name, sku))

    if not update_data:
        print("No data to update.")
        conn.close()
        return

    update_sql = f"UPDATE {DAZ_EXTRACTED_PRODUCT_TABLE} SET name = ? WHERE sku = ?"
    cursor.executemany(update_sql, update_data)
    conn.commit()

    update_count = cursor.rowcount
    print(f"\n--- Backfill Complete ---")
    print(f"Successfully updated the 'name' for {update_count} rows in the database.")

    # --- Step 4: Final verification (optional) ---
    cursor.execute(f"SELECT COUNT(*) FROM {DAZ_EXTRACTED_PRODUCT_TABLE} WHERE name IS NOT NULL")
    final_count = cursor.fetchone()[0]
    print(f"Verification: There are now {final_count} rows with a non-null name.")

    conn.close()


if __name__ == "__main__":
    backfill_product_names()
