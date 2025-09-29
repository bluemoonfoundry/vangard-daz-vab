# src/rebuild_chroma.py

import os
import sqlite3

import chromadb
from dotenv import load_dotenv

# Import the new embedding utility
from utilities import sqlite_db, CHROMA_DB_PATH, COLLECTION_NAME
from embedding_utils import generate_embeddings


def _clean_metadata(item: dict) -> dict:
    """Ensures all metadata values are of a type supported by ChromaDB."""
    clean = {}
    for key, value in item.items():
        if value is not None and isinstance(value, (str, int, float, bool)):
            clean[key] = value
    return clean

def load_sqlite_to_chroma(checkpoint_date: str|None=None, rebuild:bool = False):
    """
    If rebuild then reads all products from an SQLite database, generates new embeddings,
    and completely rebuilds the ChromaDB collection. Otherwise, only update the Chroma
    database with products that are newer than the checkpoint_date

    """
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)

    # --- 1. Rebuild the ChromaDB if rebuild is True
    if rebuild:
        print(f"Resetting and preparing ChromaDB collection: '{COLLECTION_NAME}'")
        # Delete the old collection to ensure a clean rebuild
        try:
            client.delete_collection(name=COLLECTION_NAME)
        except Exception as e:
            print(
                f"WARNING: Collection {COLLECTION_NAME} not found, creating new one: {str(e)}"
            )

    collection = client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},  # Example for 768-dim embeddings
    )

    # --- 2. Read all products or products extracted more recently then checkpoint_date

    if checkpoint_date is None:
        products_to_load = sqlite_db.execute_select_query ("SELECT * FROM product")
    else:
        products_to_load = sqlite_db.execute_select_query(
                f"SELECT * FROM product WHERE (last_updated > {checkpoint_date} OR enriched_at > {checkpoint_date}) AND embedding_text IS NOT NULL"
            )

    # print(f"Connecting to SQLite database: '{SQLITE_DB_PATH}'")
    # try:
    #     conn = sqlite3.connect(SQLITE_DB_PATH)
    #     conn.row_factory = sqlite3.Row
    #     cursor = conn.cursor()
        
    #     if checkpoint_date is None:
    #         cursor.execute("SELECT * FROM product")
    #     else:
    #         cursor.execute(
    #             "SELECT * FROM product WHERE (last_updated > ? OR enriched_at > ?) AND embedding_text IS NOT NULL",
    #             (checkpoint_date, checkpoint_date),
    #         )
    #     products_to_load = cursor.fetchall()
    #     conn.close()
    #     print(f"Found {len(products_to_load)} products in the SQLite database.")
    # except sqlite3.OperationalError as e:
    #     print(f"Error reading from SQLite: {e}")
    #     return

    # --- 3. Prepare Data for Batch Processing ---
    valid_products = [dict(row) for row in products_to_load if row["embedding_text"]]

    if not valid_products:
        print("No products with embedding_text found. Aborting.")
        return
    
    texts_to_embed = [p["embedding_text"] for p in valid_products]
    ids_to_upsert = [str(p["sku"]) for p in valid_products]
    metadatas_to_upsert: list[dict[str, str | int | float | bool | None]] = [
        _clean_metadata(p) for p in valid_products
    ]
    documents_to_upsert = [p["embedding_text"] for p in valid_products]

    # --- 4. Generate All Embeddings in a Single Batch ---
    print(f"Generating embeddings for {len(texts_to_embed)} documents...")
    # 'is_query=False' tells the utility these are documents for storage
    embedding_list = generate_embeddings(texts_to_embed, is_query=False).tolist()
    print("Embeddings generated successfully.")

    # --- 5. Prepare Metadata and IDs ---
    #ids = [str(p["sku"]) for p in valid_products]
    #documents = [p["embedding_text"] for p in valid_products]

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
            ids=ids_to_upsert,
            embeddings=embedding_list, 
            documents=documents_to_upsert, 
            metadatas=metadatas_to_upsert, 
        )
        print(f"\n--- Success! ---")
        print(
            f"Upserted {len(ids_to_upsert)} documents into ChromaDB collection '{COLLECTION_NAME}'."
        )
    except Exception as e:
        print(f"An error occurred while publishing to ChromaDB: {e}")


if __name__ == "__main__":
    load_sqlite_to_chroma()
