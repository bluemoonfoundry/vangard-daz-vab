# src/database_utils.py

import os
import sqlite3

import chromadb
from dotenv import load_dotenv


############################## UNUSED FILE #####################################

# Import the corrected embedding utility
from utilities import SQLITE_DB_PATH, CHROMA_DB_PATH, COLLECTION_NAME
from embedding_utils import generate_embeddings


def _clean_metadata(item: dict) -> dict:
    """Ensures all metadata values are of a type supported by ChromaDB."""
    clean = {}
    for key, value in item.items():
        if value is not None and isinstance(value, (str, int, float, bool)):
            clean[key] = value
    return clean


def load_sqlite_to_chroma(checkpoint_date: str):
    """
    Finds new/updated products in SQLite, generates embeddings for them,
    and upserts them into ChromaDB.
    """
    print(
        f"Loading products from '{SQLITE_DB_PATH}' updated after {checkpoint_date}..."
    )
    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Select rows updated or enriched after the checkpoint
        # We also ensure embedding_text is not null.
        cursor.execute(
            "SELECT * FROM product WHERE (last_updated > ? OR enriched_at > ?) AND embedding_text IS NOT NULL",
            (checkpoint_date, checkpoint_date),
        )
        products_to_load = [dict(row) for row in cursor.fetchall()]
        conn.close()
    except sqlite3.OperationalError as e:
        print(f"Error reading from SQLite: {e}")
        return

    if not products_to_load:
        print("No new or updated products to load into ChromaDB.")
        return

    print(f"Found {len(products_to_load)} products to process and load.")

    # 1. Prepare data for embedding and for ChromaDB
    texts_to_embed = [p["embedding_text"] for p in products_to_load]
    ids_to_upsert = [str(p["sku"]) for p in products_to_load]
    metadatas_to_upsert: list[dict[str, str | int | float | bool | None]] = [
        _clean_metadata(p) for p in products_to_load
    ]
    documents_to_upsert = [p["embedding_text"] for p in products_to_load]

    # 2. Generate new embeddings ONLY for these new products
    print(f"Generating embeddings for {len(texts_to_embed)} new/updated documents...")
    new_embeddings = generate_embeddings(texts_to_embed, is_query=False).tolist()
    print("Embeddings generated successfully.")

    # 3. Connect to ChromaDB and upsert the complete data
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    collection = client.get_or_create_collection(
        name=COLLECTION_NAME, metadata={"hnsw:space": "cosine"}
    )

    collection.upsert(
        ids=ids_to_upsert,
        embeddings=new_embeddings,
        metadatas=metadatas_to_upsert,
        documents=documents_to_upsert,
    )

    print(
        f"--- Successfully upserted {len(ids_to_upsert)} documents into ChromaDB. ---"
    )


# This function remains unchanged and correct.
def publish_to_chroma(scraped_items):
    # This was used by an older script and can be refactored or removed,
    # but for now we leave it. The `load_sqlite_to_chroma` is the primary function.
    pass
