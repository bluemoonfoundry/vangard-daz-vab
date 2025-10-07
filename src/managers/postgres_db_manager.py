import sqlite3
import os
import argparse
import json
from datetime import datetime
from utilities import fetch_json_from_url, fetch_html_content
from managers.managers import chroma_db_manager, sqlite_db
from embedding_utils import generate_embeddings
import re
from collections import Counter
import pprint


import sqlite3
import os
import argparse
from datetime import datetime
import re
from collections import Counter

# Third-party libraries
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
import chromadb
import numpy as np
# from sentence_transformers import SentenceTransformer # Uncomment for real embeddings

# --- CONSTANTS ---
SQLITE_DB_PATH = 'enriched_products.db'
SQLITE_TABLE_NAME = 'enriched_products'
BATCH_SIZE = 512


# ==============================================================================
#  PHASE 1: EXTRACT DATA FROM POSTGRESQL
# ==============================================================================
class DazDBAnalyzer:
    """
    Connects to the Daz Content PostgreSQL database to extract product data.
    """
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

    def __init__(self, db_config):
        self.db_config = db_config

    def _execute_query(self, sql, params=None):
        results = []
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, params)
                    if cur.description:
                        results = cur.fetchall()
        except psycopg2.Error as e:
            print(f"PostgreSQL Database error: {e}")
            return None
        return [dict(row) for row in results]

    def get_all_skus(self):
        """
            Efficiently fetches a list of all non-null AND non-empty SKUs from PostgreSQL.
        """
        print("Fetching all valid SKUs from PostgreSQL...")
        # This query now filters out both NULL and empty strings
        sql = "SELECT token FROM dzcontent.product WHERE token IS NOT NULL AND token != ''"
        results = self._execute_query(sql)
        return [row['token'] for row in results] if results else []

    def get_products_by_sku_list(self, skus):
        """Fetches full product data for a given list of SKUs, handling batching."""
        if not skus:
            return []
        print(f"Fetching full data for {len(skus)} products from PostgreSQL...")
        all_products = []
        for i in range(0, len(skus), BATCH_SIZE):
            sku_batch = skus[i:i + BATCH_SIZE]
            placeholders = ','.join(['%s'] * len(sku_batch))
            where_clause = f"WHERE p.token IN ({placeholders})"
            sql = f"{self.QUERY_BODY} {where_clause} {self.QUERY_GROUPING}"
            batch_results = self._execute_query(sql, tuple(sku_batch))
            if batch_results:
                all_products.extend(batch_results)
        return all_products

# ==============================================================================
#  PHASE 2: TRANSFORM AND LOAD DATA
# ==============================================================================
def setup_sqlite_db(db_path, table_name):
    """Creates the SQLite database and table using the final schema."""
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f'''
        CREATE TABLE IF NOT EXISTS {table_name} (
            sku TEXT PRIMARY KEY, url TEXT, image_url TEXT, store TEXT, name TEXT, artist TEXT, price TEXT, description TEXT,
            tags TEXT, formats TEXT, poly_count TEXT, textures_info TEXT, required_products TEXT, compatible_figures TEXT,
            compatible_software TEXT, embedding_text TEXT, last_updated TEXT, category TEXT, subcategories TEXT, styles TEXT,
            inferred_tags TEXT, enriched_at TEXT, mature INTEGER
        )
    ''')
    conn.commit()
    conn.close()
    print(f"SQLite database '{db_path}' and table '{table_name}' are ready.")

def get_all_skus_from_sqlite(db_path, table_name):
    """Efficiently fetches all existing SKUs from the SQLite database."""
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute(f"SELECT sku FROM {table_name}")
    results = [row[0] for row in cursor.fetchall()]
    conn.close()
    return results

def determine_categories(content_type_string: str) -> dict:
    """Analyzes a content type string to determine a primary category and subcategories."""
    IGNORE_WORDS = {'follower', 'default', 'support', 'preset', 'people', 'genesis', 'genesis 9', 'genesis 8', 'genesis 3'}
    PRIORITY_WORDS = {'character', 'clothes', 'accessories', 'environments', 'hair', 'poses', 'animations', 'props', 'tools', 'effects'}
    if not content_type_string: return {'category': None, 'subcategories': []}
    words = re.split(r'[^a-zA-Z0-9]+', content_type_string)
    valid_words = [w.lower().strip() for w in words if w.lower().strip() and w.lower().strip() not in IGNORE_WORDS]
    if not valid_words: return {'category': None, 'subcategories': []}
    primary_category = next((word for word in valid_words if word in PRIORITY_WORDS), None)
    if primary_category is None:
        word_counts = Counter(valid_words)
        if word_counts: primary_category = word_counts.most_common(1)[0][0]
    if primary_category is None: return {'category': None, 'subcategories': []}
    unique_words = set(valid_words)
    unique_words.discard(primary_category)
    return {'category': primary_category, 'subcategories': sorted(list(unique_words))}

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
    Generates a rich, descriptive paragraph for the embedding model.
    Focuses on combining factual data with potential use-cases and avoids noisy data.
    """
    print("    -> Generating high-quality embedding text...")
    
    # Extract clean data, providing sensible defaults
    name = product_data.get('product_name', 'a 3D asset')
    artist = product_data.get('artists')
    categories = product_data.get('categories')
    web_desc = web_data.get('description', '').strip()

    # --- Build the descriptive text part by part ---
    
    # Start with a clear, factual statement.
    parts = [f"A 3D asset package titled '{name}'."]
    if artist:
        parts.append(f"Created by the artist or studio: {artist}.")

    # Use the categories to add rich, contextual information about the product's use-case.
    if categories:
        # Clean up the category string for better sentence flow
        clean_categories = categories.replace(',', ', ')
        parts.append(f"It is categorized under: {clean_categories}.")
        
        # Add inferred use-case sentences based on keywords in categories. This is very powerful.
        cat_lower = categories.lower()
        if 'props' in cat_lower or 'decor' in cat_lower:
            parts.append("This is a set of props suitable for decorating digital scenes, environments, and dioramas.")
        if 'furniture' in cat_lower:
            parts.append("It includes furniture items for interior design and architectural visualization.")
        if 'character' in cat_lower:
            parts.append("This is a character asset for digital art and animation.")
        if 'hair' in cat_lower:
            parts.append("This is a hairstyle asset for 3D characters.")
        if 'wardrobe' in cat_lower or 'clothes' in cat_lower:
            parts.append("It contains clothing or wardrobe items for 3D figures.")
            
    # Add the high-quality human-written description from the web at the end.
    if web_desc:
        parts.append(f"Product Description: {web_desc}")

    # Join all the parts into a single, cohesive paragraph.
    return " ".join(parts)

# ==============================================================================
#  PHASE 3: GENERATE AND STORE EMBEDDINGS
# ==============================================================================
def generate_and_store_embeddings(processed_skus):
    """
    Fetches processed data from SQLite, generates embeddings, and stores them in ChromaDB
    in safe-sized batches to avoid database parameter limits.
    """
    if not processed_skus:
        print("\nNo new products were processed, skipping embedding generation.")
        return

    print(f"\n--- Starting Embedding Generation for {len(processed_skus)} products ---")

    
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

        for x, row in enumerate(rows_to_embed):
            id = row['sku']
            if id == "":                
                print (f'Row {x} = {row}')
                raise ValueError("SKU cannot be empty string")
        
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
        texts_to_embed = documents #[p["embedding_text"] for p in documents]

        print(f"Generating embeddings for {len(texts_to_embed)} documents...")
        embeddings = generate_embeddings(texts_to_embed, is_query=False).tolist()

        # 4. STORE IN CHROMADB IN A BATCH
        print("Upserting batch into ChromaDB...")

        # # Validate the ids?
        # DEBUG_SKU='24106'
        # print(f"  [DEBUG] Preparing to process SKU '{DEBUG_SKU}'. Data is:")
        # try:
        #     item_index = ids.index(DEBUG_SKU)
        #     print(f"    - ID: {ids[item_index]}")
        #     print(f"    - Document: {documents[item_index][:100]}...") # Print first 100 chars
        #     print(f"    - Metadata: {metadatas[item_index]}")
        # except (ValueError, IndexError) as e:
        #     print(f"    - Error finding debug SKU in prepared lists: {e}")

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
# ==============================================================================
#  MAIN ORCHESTRATOR
# ==============================================================================
def main():
    """Main ETL and Embedding pipeline with command-line arguments."""
    
    parser = argparse.ArgumentParser(description="ETL and Embedding process for Daz Content.")
    parser.add_argument('--force', action='store_true', help="Force a complete rebuild of the SQLite database (implies --all).")
    parser.add_argument('--all', action='store_true', help="Process all products from Postgres, not just new ones.")
    parser.add_argument('--limit', type=int, help="Process only a limited number of products. Ideal for testing.")
    parser.add_argument('--phase', type=str, choices=['etl', 'embed', 'all'], default='all', help="Run only a specific phase: 'etl', 'embed', or both if omitted.")
    args = parser.parse_args()

    if args.force and os.path.exists(SQLITE_DB_PATH):
        print("--force flag detected. Deleting existing SQLite database.")
        os.remove(SQLITE_DB_PATH)

    setup_sqlite_db(SQLITE_DB_PATH, SQLITE_TABLE_NAME)
    load_dotenv()
    try:
        db_config = {"dbname": os.environ['DB_NAME'], "user": os.environ['DB_USER'], "password": os.environ['DB_PASS'], "host": os.environ['DB_HOST'], "port": os.environ['DB_PORT']}
    except KeyError as e: print(f"Error: Missing environment variable {e}."); return

    analyzer = DazDBAnalyzer(db_config)
    postgres_skus = analyzer.get_all_skus()
    
    skus_to_process = []
    if args.all or args.force:
        print("--all or --force flag detected. Targeting all products from PostgreSQL.")
        skus_to_process = postgres_skus
    else:
        print("Default mode: Targeting only new SKUs not found in SQLite.")
        sqlite_skus = get_all_skus_from_sqlite(SQLITE_DB_PATH, SQLITE_TABLE_NAME)

        #print (f'107451 -- In DAZ = {postgres_skus.index("107451")} -- In SQLITE = {sqlite_skus.index("107451")}')

        new_skus = list(set(postgres_skus) - set(sqlite_skus))
        skus_to_process = new_skus
        print(f"Found {len(new_skus)} new SKUs to process.")

    if args.limit:
        print(f"--limit flag detected. Limiting processing to {args.limit} SKUs.")
        skus_to_process = skus_to_process[:args.limit]

    if not skus_to_process:
        print("No products to process in this run. Exiting.")
        return
    
    successfully_processed_skus = []

    if args.phase == 'etl' or args.phase == 'all':
        print(f"\n--- Phase 1: Starting ETL for {len(skus_to_process)} products ---")
        products_to_process_data = analyzer.get_products_by_sku_list(skus_to_process)
        
        conn = sqlite3.connect(SQLITE_DB_PATH)
        cursor = conn.cursor()

        try:
            with open('.figures.json', 'r') as f:
                figure_names = json.load(f)
            print(f"Successfully loaded {len(figure_names)} figure names from .figures.json.")
        except (FileNotFoundError, json.JSONDecodeError) as e:
            print(f"Error loading .figures.json: {e}")
            print("Please ensure the file exists and is a valid JSON list of strings.")
            return    
        
        for product in products_to_process_data:
            sku = product.get('sku')
            print(f"Processing '{product.get('product_name')}' (SKU: {sku})...")

            #refactored_data = refactor_compatibility(product.get('product_compatibility'), figure_names)
            refactored_data = determine_compatibility(product, figure_names)
            
            # Combine the original categories and the refactored tags
            # filter(None, ...) cleverly removes any empty or None strings
            final_tags = ', '.join(filter(None, [
                product.get('categories'), 
                refactored_data['tags_to_append']
            ]))
            
            # --- (Transform and Load steps now use the refactored data) ---
            web_data = scrape_product_page(sku) # Placeholder
            embedding_text = generate_embedding_text(product, web_data) # Placeholder
            pg_date_iso = product['last_modified_date'].isoformat() if product['last_modified_date'] else None
            
            web_data = scrape_product_page(sku)
            embedding_text = generate_embedding_text(product, web_data)
            structured_categories = determine_categories(product.get('content_types'))
            subcategories_str = ','.join(structured_categories['subcategories'])
            pg_date_iso = product['last_modified_date'].isoformat() if product['last_modified_date'] else None
            
            data_to_insert = (
                sku, web_data.get('url'), web_data.get('image_url'), web_data.get('store'),
                product.get('product_name'), product.get('artists'), web_data.get('price'), web_data.get('description'),
                final_tags, product.get('content_types'), web_data.get('poly_count'),
                web_data.get('textures_info'), web_data.get('required_products'), product.get('product_compatibility'),
                web_data.get('compatible_software'), embedding_text, pg_date_iso, structured_categories['category'],
                subcategories_str, None, None, datetime.utcnow().isoformat(), web_data.get('mature')
            )

            data_to_insert = (
                sku,
                web_data.get('url'),
                web_data.get('image_url'),
                web_data.get('store'),
                product.get('product_name'),
                product.get('artists'),
                web_data.get('price'),
                web_data.get('description'),
                final_tags, # <-- USE THE NEW COMBINED TAGS
                product.get('content_types'),
                web_data.get('poly_count'),
                web_data.get('textures_info'),
                web_data.get('required_products'),
                refactored_data['new_compatibility'], # <-- USE THE NEW CLEAN COMPATIBILITY
                web_data.get('compatible_software'),
                embedding_text,
                pg_date_iso,
                structured_categories['category'], # category (for LLM)
                subcategories_str, # subcategories (for LLM)
                None, # styles (for LLM)
                None, # inferred_tags (for LLM)
                datetime.utcnow().isoformat(),
                web_data.get('mature')
            )
            cursor.execute(f'INSERT OR REPLACE INTO {SQLITE_TABLE_NAME} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', data_to_insert)
            conn.commit()
            successfully_processed_skus.append(sku)

        conn.close()
        print(f"\nETL Phase complete.")
    
    if args.phase == 'embed' or args.phase == 'all':
        
        if args.force or args.all:
            print("Warning: --force and --all have no effect in --re-embed mode.")

        # 1. Get all SKUs directly from our local SQLite database or use successfully_processed_skus if it exists

        if len(successfully_processed_skus) > 0:
            print(f"Using SKUs from ETL: {len(successfully_processed_skus)} SKUs.")
            skus_to_embed = successfully_processed_skus
        else:
            print(f"Fetching all existing SKUs from SQLite database: '{SQLITE_DB_PATH}'")
            skus_to_embed = get_all_skus_from_sqlite(SQLITE_DB_PATH, SQLITE_TABLE_NAME)

        # 2. Apply limit if provided
        if args.limit:
            print(f"--limit flag detected. Limiting re-embedding to {args.limit} SKUs.")
            skus_to_embed = skus_to_embed[:args.limit]
        
        if not skus_to_embed:
            print("SQLite database is empty or contains no SKUs. Nothing to re-embed.")
            return

        print(f"\n--- Phase 2: Starting Embedding Generation ---")
        generate_and_store_embeddings(skus_to_embed)

def get_all_skus_from_sqlite(db_path, table_name):
    """Efficiently fetches all existing SKUs from the SQLite database."""
    if not os.path.exists(db_path):
        return []
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    try:
        cursor.execute(f"SELECT sku FROM {table_name}")
        # fetchall returns a list of tuples, e.g., [('sku1',), ('sku2',)]
        results = [row[0] for row in cursor.fetchall()]
    except sqlite3.OperationalError as e:
        print(f"Error querying SQLite: {e}. The table might not exist yet.")
        results = []
    finally:
        conn.close()
    return results

def determine_compatibility(product_data: dict, figure_names: list) -> dict:
    """
    Determines compatible figures by checking multiple fields in order of priority:
    1. The formal 'product_compatibility' string.
    2. The product 'name'.
    3. The product 'description' from web scraping (if available).
    
    Args:
        product_data: A dictionary containing the product's raw data.
        figure_names: A list of canonical figure names to search for.

    Returns:
        A dictionary with the clean compatibility string and the original
        compatibility string to be appended to tags.
    """
    compat_str = product_data.get('product_compatibility')
    name = product_data.get('product_name')
    description = product_data.get('description', '') # Description comes from web_data

    # Use a set to automatically handle duplicates
    found_figures = set()

    # --- Heuristic 1: Check the formal compatibility string first ---
    if compat_str:
        compat_lower = compat_str.lower()
        for figure in figure_names:
            if figure.lower() in compat_lower:
                found_figures.add(figure)

    # --- Heuristic 2: If nothing found, check the product name ---
    if not found_figures and name:
        name_lower = name.lower()
        for figure in figure_names:
            # We check for the figure name as a whole word or part of a compound
            # to avoid false positives (e.g., 'Dragon' matching 'Genesis 8 Dragon Form')
            if figure.lower() in name_lower:
                found_figures.add(figure)
    
    # --- Heuristic 3: If still nothing, check the product description ---
    if not found_figures and description:
        desc_lower = description.lower()
        for figure in figure_names:
            if figure.lower() in desc_lower:
                found_figures.add(figure)

    # --- Finalize and Return ---
    new_compatibility = ', '.join(sorted(list(found_figures)))
    
    return {
        'new_compatibility': new_compatibility,
        'tags_to_append': compat_str or '' # Always use the original string for tags
    }


if __name__ == "__main__":
    main()

