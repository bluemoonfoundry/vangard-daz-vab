import os
import argparse
import json
from datetime import datetime
from utilities import fetch_json_from_url, fetch_html_content
from managers.managers import chroma_db_manager, sqlite_db, daz_pg_analyzer
from embedding_utils import generate_embeddings
import re
from collections import Counter
import pprint


import os
import argparse
from datetime import datetime
import re
from collections import Counter

from dotenv import load_dotenv
load_dotenv()


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

    BATCH_SIZE = int(os.getenv('BATCH_SIZE', '512'))

    # Loop through the list of SKUs in chunks of BATCH_SIZE
    for i in range(0, len(processed_skus), BATCH_SIZE):
        # Get the current batch of SKUs
        sku_batch = processed_skus[i:i + BATCH_SIZE]
        
        print(f"\nProcessing batch {i//BATCH_SIZE + 1} of {len(processed_skus)//BATCH_SIZE + 1} (size: {len(sku_batch)})...")

        rows_to_embed = sqlite_db.get_content_by_sku_batch(sku_batch)
        
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

def main(args):
    """Main ETL and Embedding pipeline with command-line arguments."""
    
    print (f"ARGF = {args}")

    sqlite_db.setup_sqlite_db(args.force)

    postgres_skus = daz_pg_analyzer.get_all_skus()
    
    skus_to_process = []
    if args.all or args.force:
        print("--all or --force flag detected. Targeting all products from PostgreSQL.")
        skus_to_process = postgres_skus
    else:
        print("Default mode: Targeting only new SKUs not found in SQLite.")
        sqlite_skus = sqlite_db.get_all_skus_from_sqlite()

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

    print(f"Total SKUs to process: {len(skus_to_process)}: [{args.phase}]")

    if args.phase == 'etl' or args.phase == 'all':
        print(f"\n--- Phase 1: Starting ETL for {len(skus_to_process)} products ---")
        products_to_process_data = daz_pg_analyzer.get_products_by_sku_list(skus_to_process)
        
        conn = sqlite_db.get_connection() 
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

            refactored_data = determine_compatibility(product, figure_names)
            
            # Combine the original categories and the refactored tags
            final_tags = ', '.join(filter(None, [
                product.get('categories'), 
                refactored_data['tags_to_append']
            ]))
            
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
            cursor.execute(f'INSERT OR REPLACE INTO {sqlite_db.sqlite_db_table} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)', data_to_insert)
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
            print(f"Fetching all existing SKUs from SQLite database: '{sqlite_db.sqlite_db_path}'...")
            skus_to_embed = sqlite_db.get_all_skus_from_sqlite()

        # 2. Apply limit if provided
        if args.limit:
            print(f"--limit flag detected. Limiting re-embedding to {args.limit} SKUs.")
            skus_to_embed = skus_to_embed[:args.limit]
        
        if not skus_to_embed:
            print("SQLite database is empty or contains no SKUs. Nothing to re-embed.")
            return

        print(f"\n--- Phase 2: Starting Embedding Generation ---")
        generate_and_store_embeddings(skus_to_embed)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL and Embedding process for Daz Content.")
    parser.add_argument('--force', action='store_true', help="Force a complete rebuild of the SQLite database (implies --all).")
    parser.add_argument('--all', action='store_true', help="Process all products from Postgres, not just new ones.")
    parser.add_argument('--limit', type=int, help="Process only a limited number of products. Ideal for testing.")
    parser.add_argument('--phase', type=str, choices=['etl', 'embed', 'all'], default='all', help="Run only a specific phase: 'etl', 'embed', or both if omitted.")
    args = parser.parse_args()

    main(args)

