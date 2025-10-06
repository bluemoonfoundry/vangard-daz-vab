import json
import os
from collections import Counter
from typing import List, Optional

import chromadb
from embedding_utils import generate_embeddings

#from utilities import COLLECTION_NAME, CHROMA_DB_PATH

from managers.managers import chroma_db_manager



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
    # client = chromadb.PersistentClient(path=CHROMA_DB_PATH)
    # try:
    #     collection = client.get_collection(name=COLLECTION_NAME)
    # except ValueError:
    #     print(f"Warning: ChromaDB collection '{COLLECTION_NAME}' not found.")
    #     return {"total_hits": 0, "limit": limit, "offset": offset, "results": []}

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

    results = chroma_db_manager.query(
        query_embeddings=[query_embedding.tolist()],
        n_results=query_limit,
        where=where_filter,
        include=["metadatas", "distances"],
    )

    #print (f'DEBUG: Result set = {results}')

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

    #print (f'DEBUG: Result set = {processed_results}')

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

    print (f'DEBUG: Result set = {len(paginated_results)}')

    return {
        "total_hits": len(processed_results),
        "limit": limit,
        "offset": offset,
        "results": paginated_results,
    }



