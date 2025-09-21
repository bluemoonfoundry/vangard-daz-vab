# src/inspect_chroma.py

import os
import chromadb
from dotenv import load_dotenv

def inspect_collection_metadata(limit: int = 5):
    """
    Connects to ChromaDB, fetches a few items from the collection,
    and prints the data types of their metadata fields.
    """
    load_dotenv()
    CHROMA_DB_PATH = os.getenv("CHROMA_PATH", "db")
    COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "daz_products")

    print(f"--- Connecting to ChromaDB at: {CHROMA_DB_PATH} ---")
    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    
    try:
        collection = client.get_collection(name=COLLECTION_NAME)
        print(f"--- Successfully connected to collection: '{COLLECTION_NAME}' ---")
    except ValueError:
        print(f"Error: Collection '{COLLECTION_NAME}' not found.")
        return
        
    total_docs = collection.count()
    if total_docs == 0:
        print("Collection is empty. Nothing to inspect.")
        return
    
    print(f"Collection has {total_docs} documents. Fetching {limit} for inspection...")

    # --- Fetch a few items from the collection ---
    # We include the 'metadatas' to inspect them.
    results = collection.get(
        limit=limit,
        include=["metadatas"]
    )
    
    if not results or not results['ids']:
        print("Could not retrieve any items from the collection.")
        return

    # --- Iterate and print the metadata and its type ---
    for i, item_id in enumerate(results['ids']):
        metadata = results['metadatas'][i]
        
        print("\n" + "="*50)
        print(f"INSPECTING ITEM ID: {item_id}")
        print("="*50)
        
        for key, value in metadata.items():
            # Get the Python type of the value
            value_type = type(value).__name__
            
            # Print the key, its type, and a sample of the value
            print(f"  - Key: '{key}'")
            print(f"    Type: {value_type}")
            # Truncate long values for readability
            value_sample = str(value)
            if len(value_sample) > 100:
                value_sample = value_sample[:100] + "..."
            print(f"    Value: {value_sample}")
            
    print("\n--- Inspection Complete ---")

if __name__ == "__main__":
    inspect_collection_metadata()