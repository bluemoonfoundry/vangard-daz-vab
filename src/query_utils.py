import json
import os
from collections import Counter
from typing import List, Optional

import chromadb
from dotenv import load_dotenv

# Import the centralized embedding utility
from embedding_utils import generate_embeddings


def build_where_clause(
    tags: Optional[List[str]] = None,
    artists: Optional[List[str]] = None,
    categories: Optional[List[str]] = None,
    compatible_figures: Optional[List[str]] = None,
):
    """
    Dynamically builds a ChromaDB 'where' filter for multiple fields.
    Uses $and for criteria across different fields (e.g., category AND artist)
    and $or for criteria within the same field (e.g., category is Clothing OR Hair).
    """

    # Conditions that must ALL be met are added to this list
    if not any([tags, artists, categories, compatible_figures]):
        return None

    and_conditions = []

    # Helper function to create a filter block for a list of values
    def create_or_condition(field_name: str, values: List[str]):
        if not values:
            return

        # For the 'category' field, which is a single string, we use exact match ($eq)
        if field_name == "category":
            conditions = [{field_name: {"$eq": value}} for value in values]
        # For fields stored as JSON strings of lists, we use substring matching ($contains)
        else:
            for value in values:
                print(
                    f"+++++ Test field_name: {field_name}, values: {value}, type: {type(value)}"
                )
            conditions = [{field_name: {"$contains": value}} for value in values]

        if len(conditions) > 1:
            and_conditions.append({"$or": conditions})
        elif conditions:
            and_conditions.append(conditions[0])

    # Build conditions for each filter type passed to the function
    create_or_condition("tags", tags)
    create_or_condition("artist", artists)
    create_or_condition("category", categories)
    create_or_condition("compatible_figures", compatible_figures)

    # Return the final filter structure for the ChromaDB query
    if not and_conditions:
        return None
    if len(and_conditions) == 1:
        return and_conditions[0]

    return {"$and": and_conditions}


def search(
    prompt: str,
    tags: Optional[List[str]] = None,
    artists: Optional[List[str]] = None,
    categories: Optional[List[str]] = None,
    compatible_figures: Optional[List[str]] = None,
    limit: int = 10,
    offset: int = 0,
    score_threshold: float = 2.0,  # Using a permissive default
    sort_by: str = "relevance",
    sort_order: str = "descending",
):
    """
    Performs a hybrid search with multiple, faceted metadata filters.
    """
    load_dotenv()
    CHROMA_DB_PATH = os.getenv("CHROMA_PATH", "db")
    COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "daz_products")

    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    try:
        collection = client.get_collection(name=COLLECTION_NAME)
    except ValueError:
        print(f"Warning: ChromaDB collection '{COLLECTION_NAME}' not found.")
        return {"total_hits": 0, "limit": limit, "offset": offset, "results": []}

    # --- 1. Generate Query Embedding ---
    query_embedding = generate_embeddings(prompt, is_query=True)

    # --- 2. Build the Combined Metadata Filter ---
    where_filter = build_where_clause(
        tags=tags,
        artists=artists,
        categories=categories,
        compatible_figures=compatible_figures,
    )
    if where_filter:
        print(f"DEBUG: Applying metadata filter: {where_filter}")

    # Fetch a larger number of results to allow for post-filtering, sorting, and pagination
    query_limit = (offset + limit) * 5 + 20  # A generous buffer

    # --- 3. Query ChromaDB ---
    results = collection.query(
        query_embeddings=[query_embedding.tolist()],
        n_results=query_limit,
        where=where_filter,
        include=["metadatas", "distances"],
    )

    # --- 4. Post-process Results (Filtering by Score) ---
    processed_results = []
    if results["ids"]:
        for i in range(len(results["ids"][0])):
            dist = results["distances"][0][i]
            if dist <= score_threshold:
                processed_results.append(
                    {
                        "id": results["ids"][0][i],
                        "distance": dist,
                        "metadata": results["metadatas"][0][i],
                    }
                )

    # --- 5. Sorting Logic ---
    reverse_order = sort_order == "descending"
    if sort_by != "relevance":
        # Sort by a metadata field, handling potential missing keys gracefully
        processed_results.sort(
            key=lambda x: x["metadata"].get(sort_by) or "",  # Fallback for sorting
            reverse=reverse_order,
        )
    # Note: ChromaDB already returns results sorted by relevance (distance ascending)

    # --- 6. Apply Pagination and Return ---
    paginated_results = processed_results[offset : offset + limit]

    return {
        "total_hits": len(processed_results),
        "limit": limit,
        "offset": offset,
        "results": paginated_results,
    }


def get_db_stats():
    """
    Gathers and returns statistics and histograms for all key filterable fields.
    """
    load_dotenv()
    CHROMA_DB_PATH = os.getenv("CHROMA_PATH", "db")
    COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "daz_products")

    client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    try:
        collection = client.get_collection(name=COLLECTION_NAME)
    except ValueError:
        return None

    total_docs = collection.count()
    if total_docs == 0:
        return {"total_docs": 0, "last_update": "N/A", "histograms": {}}

    all_metadatas = collection.get(include=["metadatas"])["metadatas"]

    # Initialize Counters for Histograms
    tag_counter, artist_counter, figure_counter, category_counter = (
        Counter(),
        Counter(),
        Counter(),
        Counter(),
    )

    # Find the Last Update Date
    last_update_dates = [
        meta.get("last_updated") for meta in all_metadatas if meta.get("last_updated")
    ]
    last_update = max(last_update_dates) if last_update_dates else "N/A"

    # Iterate and Process All Metadata
    for meta in all_metadatas:
        if category := meta.get("category"):
            category_counter.update([category])

        def parse_and_update_counter(field_name: str, counter: Counter):
            json_string = meta.get(field_name)
            if json_string:
                try:
                    item_list = json.loads(json_string)
                    if isinstance(item_list, list):
                        counter.update(item_list)
                except (json.JSONDecodeError, TypeError):
                    pass  # Ignore malformed data

        parse_and_update_counter("tags", tag_counter)
        parse_and_update_counter("artist", artist_counter)
        parse_and_update_counter("compatible_figures", figure_counter)

    return {
        "total_docs": total_docs,
        "last_update": last_update,
        "histograms": {
            "tags": tag_counter,
            "artists": artist_counter,
            "compatible_figures": figure_counter,
            "categories": category_counter,
        },
    }
