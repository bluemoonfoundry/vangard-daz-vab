# src/rebuild_chroma.py

import sqlite3
import json
import os
from dotenv import load_dotenv
import chromadb

# Import the new embedding utility
from embedding_utils import generate_embeddings

# --- Configuration ---


def main():
    """
    Reads all products from an SQLite database, generates new embeddings,
    and completely rebuilds the ChromaDB collection.
    """
    # --- 1. Load Configuration and Initialize Clients ---
    load_dotenv()
    CHROMA_DB_PATH = os.getenv("CHROMA_PATH", "db")
    COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "daz_products")
    SQLITE_DB_PATH =  os.getenv("SQLITE_DB_PATH", "products.db")

    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    
    print(f"Resetting and preparing ChromaDB collection: '{COLLECTION_NAME}'")
    # Delete the old collection to ensure a clean rebuild
    try:
        client.delete_collection(name=COLLECTION_NAME)
    except Exception as e:
            print (f'WARNING: Collection {COLLECTION_NAME} not found, creating new one: {str(e)}')        
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"}  # Example for 768-dim embeddings
    )

    # --- 2. Read All Products from SQLite ---
    print(f"Connecting to SQLite database: '{SQLITE_DB_PATH}'")
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM product")
        all_products = cursor.fetchall()
        conn.close()
        print(f"Found {len(all_products)} products in the SQLite database.")
    except sqlite3.OperationalError as e:
        print(f"Error reading from SQLite: {e}")
        return

    # --- 3. Prepare Data for Batch Processing ---
    valid_products = [dict(row) for row in all_products if row['embedding_text']]
    texts_to_embed = [p['embedding_text'] for p in valid_products]

    if not valid_products:
        print("No products with embedding_text found. Aborting.")
        return

    # --- 4. Generate All Embeddings in a Single Batch ---
    print(f"Generating embeddings for {len(texts_to_embed)} documents...")
    # 'is_query=False' tells the utility these are documents for storage
    embedding_list = generate_embeddings(texts_to_embed, is_query=False).tolist()
    print("Embeddings generated successfully.")

    # --- 5. Prepare Metadata and IDs ---
    ids = [str(p['sku']) for p in valid_products]
    documents = [p['embedding_text'] for p in valid_products]
    
    metadatas = []
    for item in valid_products:
        clean_meta = {}
        for key, value in item.items():
            # Ensure all metadata values are simple types for ChromaDB
            if value is not None and isinstance(value, (str, int, float, bool)):
                clean_meta[key] = value
        metadatas.append(clean_meta)

    # --- 6. Upsert the Batch into ChromaDB ---
    # Note: ChromaDB batches automatically, but we do it here for clarity.
    # For very large datasets (>10k), you might want to loop in smaller batches.
    try:
        collection.upsert(
            embeddings=embedding_list,
            documents=documents,
            metadatas=metadatas,
            ids=ids
        )
        print(f"\n--- Success! ---")
        print(f"Upserted {len(ids)} documents into ChromaDB collection '{COLLECTION_NAME}'.")
    except Exception as e:
        print(f"An error occurred while publishing to ChromaDB: {e}")

if __name__ == "__main__":
    main()