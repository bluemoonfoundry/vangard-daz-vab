# src/backfill_images.py

import sqlite3
import json
import sys

# --- Configuration ---
# The path to your existing SQLite database.
SQLITE_DB_PATH = "products.db"
# The name of the table to update.
TABLE_NAME = "product"
# The path to your JSON file containing the SKU-to-image mapping.
# This script assumes the file is in the project root.
JSON_SOURCE_FILE = "products.json" 

def backfill_image_urls():
    """
    Adds an 'image_url' column to the product table if it doesn't exist,
    then reads a JSON file and updates the new column for each matching SKU.
    """
    
    # --- Step 1: Add the new column to the database ---
    print(f"Connecting to database: {SQLITE_DB_PATH}")
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    try:
        print("Checking for 'image_url' column and adding it if it doesn't exist...")
        # This will safely add the column only if it's missing.
        # It will do nothing if the column already exists.
        cursor.execute(f"ALTER TABLE {TABLE_NAME} ADD COLUMN image_url TEXT")
        print("-> 'image_url' column added successfully.")
    except sqlite3.OperationalError as e:
        # This error is expected if the column already exists.
        if "duplicate column name" in str(e):
            print("-> 'image_url' column already exists. Skipping.")
        else:
            # Re-raise the error if it's something else
            raise e
            
    conn.commit()

    # --- Step 2: Read the source JSON file ---
    print(f"\nReading image URL data from: {JSON_SOURCE_FILE}")
    try:
        # We assume the JSON file is in the parent directory of 'src'
        json_path = f"../{JSON_SOURCE_FILE}"
        with open(json_path, 'r', encoding='utf-8') as f:
            products_data = json.load(f)
        
        # Create a fast lookup dictionary: {sku: image_url}
        image_url_map = {
            item['sku']: item['image_url'] 
            for item in products_data 
            if 'sku' in item and 'image_url' in item
        }
        print(f"Found {len(image_url_map)} products with image URLs in the JSON file.")

    except FileNotFoundError:
        print(f"Error: Source file not found at '{json_path}'. Please make sure it exists.", file=sys.stderr)
        conn.close()
        return
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error: Could not process '{JSON_SOURCE_FILE}'. Ensure it's a valid JSON array of objects with 'sku' and 'image_url' keys.", file=sys.stderr)
        print(f"Details: {e}", file=sys.stderr)
        conn.close()
        return

    # --- Step 3: Update the database records ---
    print("\nStarting database update process...")
    update_count = 0
    
    # We can update many rows efficiently with executemany
    update_data = []
    for sku, image_url in image_url_map.items():
        update_data.append((image_url, sku))

    if not update_data:
        print("No data to update.")
        conn.close()
        return

    update_sql = f"UPDATE {TABLE_NAME} SET image_url = ? WHERE sku = ?"
    cursor.executemany(update_sql, update_data)
    conn.commit()
    
    update_count = cursor.rowcount
    print(f"\n--- Backfill Complete ---")
    print(f"Successfully updated {update_count} rows in the database.")
    
    # --- Step 4: Final verification (optional) ---
    cursor.execute(f"SELECT COUNT(*) FROM {TABLE_NAME} WHERE image_url IS NOT NULL")
    final_count = cursor.fetchone()[0]
    print(f"Verification: There are now {final_count} rows with a non-null image_url.")
    
    conn.close()

if __name__ == "__main__":
    backfill_image_urls()