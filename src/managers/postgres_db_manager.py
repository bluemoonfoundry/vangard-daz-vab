import sqlite3
import os
import argparse
from datetime import datetime
from utilities import fetch_json_from_url, fetch_html_content
from managers.managers import chroma_db_manager
from embedding_utils import generate_embeddings
import re
from collections import Counter
import pprint

# --- PREVIOUS IMPORTS AND DAZDBANALYZER CLASS ---
# (The DazDBAnalyzer class remains exactly the same as before.
#  It's responsible for the "Extract" step from PostgreSQL.)

import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

class DazDBAnalyzer:
    # ... (paste the full, unchanged DazDBAnalyzer class here) ...
    QUERY_BODY = """
        SELECT
            p.id AS product_id,
            p.name AS product_name,
            p.artists,
            p.token AS sku,
            COALESCE(p.last_update, p.date_installed) AS last_modified_date,
            REGEXP_REPLACE(
                STRING_AGG(DISTINCT final_compat.compatibility_name, ', '),
                '^, *| *, $', ''
            ) AS product_compatibility,
            COUNT(DISTINCT c.id) AS content_item_count,
            STRING_AGG(DISTINCT cat."fldCategoryName", ', ') AS categories,
            STRING_AGG(DISTINCT ct."fldType", ', ') AS content_types
        FROM dzcontent.product AS p
        LEFT JOIN dzcontent.content AS c ON p.id = c.product_id
        LEFT JOIN (
            SELECT
                c_inner.id AS content_id,
                COALESCE(
                    cb."fldCompatibilityBase",
                    CASE
                        WHEN c_inner.path ILIKE '%%/Genesis 9/%%' OR c_inner.filename ILIKE '%%Genesis 9%%' OR c_inner.filename ILIKE '%%G9%%' THEN 'Genesis 9'
                        WHEN c_inner.path ILIKE '%%/Genesis 8.1/%%' OR c_inner.filename ILIKE '%%Genesis 8.1%%' OR c_inner.filename ILIKE '%%G8_1%%' THEN 'Genesis 8.1'
                        WHEN c_inner.path ILIKE '%%/Genesis 8/%%' OR c_inner.filename ILIKE '%%Genesis 8%%' OR c_inner.filename ILIKE '%%G8%%' THEN 'Genesis 8'
                        WHEN c_inner.path ILIKE '%%/Genesis 3/%%' OR c_inner.filename ILIKE '%%Genesis 3%%' OR c_inner.filename ILIKE '%%G3%%' THEN 'Genesis 3'
                        WHEN c_inner.path ILIKE '%%/Genesis 2/%%' OR c_inner.filename ILIKE '%%Genesis 2%%' OR c_inner.filename ILIKE '%%G2%%' THEN 'Genesis 2'
                        ELSE NULL
                    END
                ) AS compatibility_name
            FROM dzcontent.content AS c_inner
            LEFT JOIN dzcontent.compatibility_base_content AS cbc ON c_inner.id = cbc.content_id
            LEFT JOIN dzcontent."tblCompatibilityBase" AS cb ON cbc.compatibility_base_id = cb."RecID"
        ) AS final_compat ON c.id = final_compat.content_id
        LEFT JOIN dzcontent.category_content AS cc ON c.id = cc.content_id
        LEFT JOIN dzcontent."tblCategories" AS cat ON cc.category_id = cat."RecID"
        LEFT JOIN dzcontent."tblType" AS ct ON c.content_type_id = ct."RecID"
    """
    QUERY_GROUPING = "GROUP BY p.id, p.name, p.artists, p.token, p.last_update, p.date_installed ORDER BY p.name"
    def __init__(self, db_config): self.db_config = db_config
    def _execute_query(self, sql, params=None):
        results = []
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, params)
                    results = cur.fetchall()
        except psycopg2.Error as e: print(f"Database error: {e}"); return None
        return [dict(row) for row in results]
    def get_all_products(self):
        print("Fetching data for all products..."); sql = f"{self.QUERY_BODY} {self.QUERY_GROUPING}"; return self._execute_query(sql)
    def get_product_by_sku(self, sku):
        print(f"Fetching data for product with SKU: {sku}..."); where_clause = "WHERE p.token = %s"; sql = f"{self.QUERY_BODY} {where_clause} {self.QUERY_GROUPING}"; results = self._execute_query(sql, (sku,)); return results[0] if results else None
    def get_new_or_updated_products(self, since_date):
        print(f"Fetching products modified since {since_date.strftime('%Y-%m-%d %H:%M:%S')}..."); where_clause = "WHERE COALESCE(p.last_update, p.date_installed) > %s"; sql = f"{self.QUERY_BODY} {where_clause} {self.QUERY_GROUPING}"; return self._execute_query(sql, (since_date,))


# --- CONSTANTS FOR THE ETL PROCESS ---
SQLITE_DB_PATH = 'enriched_products.db'
SQLITE_TABLE_NAME = 'enriched_products'

# --- ETL HELPER FUNCTIONS ---

def setup_sqlite_db(db_path, table_name):
    """Creates the SQLite database and table using your specific schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    # Using an f-string is safe here as the table name is from a constant, not user input.
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            sku TEXT PRIMARY KEY,
            url TEXT,
            image_url TEXT,
            store TEXT,
            name TEXT,
            artist TEXT,
            price TEXT,
            description TEXT,
            tags TEXT,
            formats TEXT,
            poly_count TEXT,
            textures_info TEXT,
            required_products TEXT,
            compatible_figures TEXT,
            compatible_software TEXT,
            embedding_text TEXT,
            last_updated TEXT,
            category TEXT,
            subcategories TEXT,
            styles TEXT,
            inferred_tags TEXT,
            enriched_at TEXT,
            mature INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print(f"SQLite database '{db_path}' and table '{table_name}' are ready.")

def scrape_product_page(sku):
    """
    --- PLACEHOLDER ---
    This is where your web scraping logic will go.
    It should take a SKU, construct a URL, fetch the page, and parse it.
    """
    #print(f"    -> (Placeholder) Scraping web for SKU: {sku}")
    rv = {}
    slab_url = f'https://www.daz3d.com/dazApi/slab/{sku}'
    base_content = fetch_json_from_url(slab_url)
    if base_content is not None:
        mark_url = base_content['url']
        if mark_url.startswith("/"):
            mark_url=mark_url[1:]
        product_page_url = f'https://www.daz3d.com/{mark_url}'
        x = base_content['imageUrl']
        image_url = x[x.index('/http')+1:]
        prices = base_content['prices']
        if "USD" in prices:
            price = prices['USD']
        else:
            price = "Unknown"

        rv = {
            'url': product_page_url,
            'image_url': image_url,
            'price': f"${price}",
            'mature': base_content['mature']
        }

        # Now go to the product page. The main thing we need from that page is a description
        html_content, tags = fetch_html_content(product_page_url)
        if tags is not None:
            rv['description'] = tags['og:description']
            rv['tags'] = tags['keywords']

    return rv

def generate_embedding_text(product_data, web_data):
    """
    --- PLACEHOLDER ---
    This is where your LLM/AI logic will go.
    It takes the combined data and generates the description for vectorization.
    """
    #print("    -> (Placeholder) Generating embedding text...")
    # Example: Combine fields to create a rich description
    desc = (
        f"Product: {product_data.get('product_name')}. "
        f"By artist: {product_data.get('artists')}. "
        f"Compatible with: {product_data.get('product_compatibility')}. "
        f"Categories: {product_data.get('categories')}. "
        f"Web description: {web_data.get('description')}"
    )
    return desc

def generate_and_store_embeddings(processed_skus):
    """
    Fetches processed data from SQLite, generates embeddings, and stores them in ChromaDB
    in safe-sized batches to avoid database parameter limits.
    """
    if not processed_skus:
        print("\nNo new products were processed, skipping embedding generation.")
        return

    print(f"\n--- Starting Embedding Generation for {len(processed_skus)} products ---")

    # Define a safe batch size, well below the typical 999 limit
    BATCH_SIZE = 900
    
    # Connect to ChromaDB once at the beginning
    # print("Connecting to ChromaDB...")
    # client = chromadb.Client() # For persistent storage, use: chromadb.PersistentClient(path="/path/to/db")
    # collection = client.get_or_create_collection("product_embeddings")

    # Loop through the list of SKUs in chunks of BATCH_SIZE
    for i in range(0, len(processed_skus), BATCH_SIZE):
        # Get the current batch of SKUs
        sku_batch = processed_skus[i:i + BATCH_SIZE]
        
        print(f"\nProcessing batch {i//BATCH_SIZE + 1} of {len(processed_skus)//BATCH_SIZE + 1} (size: {len(sku_batch)})...")

        # 1. CONNECT TO SQLITE AND FETCH DATA FOR THE CURRENT BATCH
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        placeholders = ','.join(['?'] * len(sku_batch))
        
        sqlite_query = f"""
            SELECT sku, url, image_url, embedding_text, name, artist, compatible_figures, tags, category, subcategories
            FROM {SQLITE_TABLE_NAME}
            WHERE sku IN ({placeholders})
        """
        
        cursor.execute(sqlite_query, sku_batch)
        rows_to_embed = cursor.fetchall()
        conn.close()
        
        print(f"Fetched {len(rows_to_embed)} rows from SQLite for embedding.")
        
        if not rows_to_embed:
            continue # Should not happen, but good practice

        # 2. PREPARE DATA FOR BATCH PROCESSING
        ids = [row['sku'] for row in rows_to_embed]
        documents = [row['embedding_text'] for row in rows_to_embed]
        metadatas = [
            {
                "sku": row["sku"],
                "url": row["url"],
                "image_url": row["image_url"],
                "name": row['name'], 
                "artist": row['artist'], 
                "compatibility": row['compatible_figures'], 
                "tags": row['tags'],
                "category": row['category'],
                "subcategories": row['subcategories']
            } 
            for row in rows_to_embed
        ]

        # 3. GENERATE EMBEDDINGS IN A BATCH
        print("    -> (Placeholder) Generating embeddings for the current batch...")
        import numpy as np
        embeddings = np.random.rand(len(documents), 3).tolist() 
        print(f"Generated {len(embeddings)} embedding vectors for this batch.")


        texts_to_embed = documents #[p["embedding_text"] for p in documents]

        print(f"Generating embeddings for {len(texts_to_embed)} documents...")
        embeddings = generate_embeddings(texts_to_embed, is_query=False).tolist()

        # 4. STORE IN CHROMADB IN A BATCH
        print("Upserting batch into ChromaDB...")
        try:
            chroma_db_manager.collection.upsert(
                ids=ids,
                embeddings=embeddings, 
                metadatas=metadatas,
                documents=documents                
            )
            print(f"\n--- Success! ---")
            print(
                f"Upserted {len(ids)} documents into ChromaDB collection '{chroma_db_manager.collection_name}'."
            )
        except Exception as e:
            print(f"An error occurred while publishing to ChromaDB: {e}")
            return False
    
    print(f"\nSuccessfully finished processing all batches.")

def determine_categories(content_type_string: str) -> dict:
    """
    Analyzes a content type string to determine a primary category and subcategories.
    It first checks for priority words; if none are found, it falls back to
    determining the category based on word frequency.

    Args:
        content_type_string: A string containing comma-separated paths,
                             e.g., "Follower/Wardrobe/Pant, People/Genesis 9/Hair".

    Returns:
        A dictionary with 'category' and 'subcategories' keys.
    """
    # --- 1. Define Constant Sets ---
    # Using sets for fast O(1) average time complexity lookups.
    IGNORE_WORDS = {'follower', 'default', 'support', 'preset', 'people', 'genesis', 'genesis 9', 'genesis 8', 'genesis 3'}
    
    PRIORITY_WORDS = {
        'character', 'clothes', 'accessories', 'environments', 'hair', 'poses', 
        'animations', 'props', 'tools', 'effects'
    }

    if not content_type_string:
        return {'category': None, 'subcategories': []}

    # --- 2. Parse and Clean Words ---
    words = re.split(r'[^a-zA-Z0-9]+', content_type_string)
    valid_words = [
        word.lower().strip()
        for word in words
        if word.lower().strip() and word.lower().strip() not in IGNORE_WORDS
    ]

    if not valid_words:
        return {'category': None, 'subcategories': []}

    # --- 3. Determine the Primary Category ---
    primary_category = None

    # Phase 1: Priority Scan
    # Find the first valid word that is in our priority list.
    for word in valid_words:
        if word in PRIORITY_WORDS:
            primary_category = word
            break  # Found a priority word, lock it in and stop searching.

    # Phase 2: Frequency Fallback
    # If no priority word was found after checking all valid words...
    if primary_category is None:
        word_counts = Counter(valid_words)
        # ...fall back to the most common word.
        if word_counts:
            primary_category = word_counts.most_common(1)[0][0]

    # If for some reason we still don't have a category, exit gracefully.
    if primary_category is None:
        return {'category': None, 'subcategories': []}

    # --- 4. Determine Subcategories ---
    unique_words = set(valid_words)
    # Use .discard() instead of .remove() as it doesn't raise an error if the item isn't found
    unique_words.discard(primary_category)
    subcategories = sorted(list(unique_words))

    # --- 5. Return the Final Structure ---
    return {
        'category': primary_category,
        'subcategories': subcategories
    }

def main():
    """Main ETL (Extract, Transform, Load) loop with command-line arguments."""
    
    # --- Setup Command-Line Argument Parsing ---
    parser = argparse.ArgumentParser(description="ETL process for Daz Content Database.")
    parser.add_argument(
        '--force',
        action='store_true',  # This makes it a boolean flag
        help="Force a complete rebuild of the SQLite database, ignoring dates."
    )
    parser.add_argument(
        '--limit',
        type=int,
        help="Process only the first N products. Ideal for testing."
    )
    args = parser.parse_args()

    # --- Handle --force flag logic: Delete existing DB if it exists ---
    if args.force:
        print("--force flag detected. The existing SQLite database will be rebuilt.")
        if os.path.exists(SQLITE_DB_PATH):
            os.remove(SQLITE_DB_PATH)
            print(f"Deleted existing database at '{SQLITE_DB_PATH}'.")

    # 1. Setup the local destination database
    setup_sqlite_db(SQLITE_DB_PATH, SQLITE_TABLE_NAME)

    # 2. Extract: Get all products from Postgres
    load_dotenv()
    try:
        db_config = { "dbname": os.environ['DB_NAME'], "user": os.environ['DB_USER'], "password": os.environ['DB_PASS'], "host": os.environ['DB_HOST'], "port": os.environ['DB_PORT'] }
    except KeyError as e: print(f"Error: Missing environment variable {e}. Please check your .env file."); return

    analyzer = DazDBAnalyzer(db_config)
    all_postgres_products = analyzer.get_all_products()
    if all_postgres_products is None: print("Failed to retrieve products from PostgreSQL. Aborting."); return

    # --- Handle --limit flag logic: Slice the list of products to process ---
    products_to_process = all_postgres_products
    if args.limit:
        print(f"--limit flag detected. Processing the first {args.limit} products only.")
        products_to_process = all_postgres_products[:args.limit]

    # 3. Connect to SQLite for the processing loop
    conn = sqlite3.connect(SQLITE_DB_PATH)
    cursor = conn.cursor()

    processed_skus_in_this_run = []   
    for product in products_to_process:
        sku = product.get('sku')
        if not sku:
            print(f"Skipping '{product.get('product_name')}' (ID: {product.get('product_id')}) - no SKU available.")
            continue

        # --- Conditional Date Check (Bypassed by --force) ---
        if not args.force:
            cursor.execute(f"SELECT last_updated FROM {SQLITE_TABLE_NAME} WHERE sku = ?", (product['sku'],))
            result = cursor.fetchone()
            pg_date = product.get('last_modified_date')
            if result and result[0] and pg_date and datetime.fromisoformat(result[0]) >= pg_date:
                print(f"Skipping '{product.get('product_name')}' (SKU: {product.get('sku')}) - already up to date.")
                continue

        print(f"Processing '{product.get('product_name')}' (SKU: {product.get('sku')})...")
        
        # ... (Transform and Load steps are identical to the previous script) ...
        web_data = scrape_product_page(product['sku'])
        embedding_text = generate_embedding_text(product, web_data)
        pg_date_iso = product['last_modified_date'].isoformat() if product['last_modified_date'] else None
        
        categories = determine_categories(product.get('content_types'))

        data_to_insert = (
            product['sku'], 
            web_data.get('url'), 
            web_data.get('image_url'), 
            web_data.get('store'),
            product.get('product_name'), 
            product.get('artists'), 
            web_data.get('price'), 
            web_data.get('description'),
            web_data.get('tags'), 
            product.get('content_types'), 
            web_data.get('poly_count'),
            web_data.get('textures_info'), 
            web_data.get('required_products'), 
            product.get('product_compatibility'),
            web_data.get('compatible_software'), 
            embedding_text, 
            pg_date_iso, 
            categories['category'], # category
            ",".join(categories['subcategories']), # cubcategories
            None, # styles
            None, # inferred_tags
            datetime.utcnow().isoformat(), # enriched_at
            web_data.get('mature') # mature
        )

        
        
        cursor.execute(f'''
            INSERT OR REPLACE INTO {SQLITE_TABLE_NAME} (
                sku, url, image_url, store, name, artist, price, description, tags, formats,
                poly_count, textures_info, required_products, compatible_figures,
                compatible_software, embedding_text, last_updated, category, subcategories,
                styles, inferred_tags, enriched_at, mature
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', data_to_insert)
        
        conn.commit()
        processed_skus_in_this_run.append(sku)

    conn.close()

    generate_and_store_embeddings(processed_skus_in_this_run)

    print("\nProcessing complete.")
    print(f"Total products processed in this run: {len(products_to_process)}")

if __name__ == "__main__":
    main()