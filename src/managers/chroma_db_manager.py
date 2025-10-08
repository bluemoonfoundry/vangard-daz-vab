import chromadb
import json
import re
from collections import Counter
from typing import List, Optional
from embedding_utils import generate_embeddings

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


class ChromaDbManager:

    def __init__(self, chroma_db_path, collection_name):
        self.chroma_db_path = chroma_db_path
        self.collection_name = collection_name
        self.client = chromadb.PersistentClient(path=chroma_db_path)
        self.collection = self.client.get_or_create_collection(
            name=self.collection_name,
            metadata={"hnsw:space": "cosine"},  # Example for 768-dim embeddings
        )

        print ('Opening Chroma DB:')
        print (f' db path ............. [ {self.chroma_db_path}]')
        print (f' collection........... [ {self.collection_name}]')
        
        
    def _clean_metadata(self, item: dict) -> dict:
        """Ensures all metadata values are of a type supported by ChromaDB."""
        clean = {}
        for key, value in item.items():
            if value is not None and isinstance(value, (str, int, float, bool)):
                clean[key] = value
        return clean
    
    def delete_collection(self, collection_name=None):
        try:
            if collection_name is None:
                cname = self.collection_name
            else:
                cname = collection_name 

            self.client.delete_collection(name=cname)
        except Exception as e:
           raise e

    def search(
        self,
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

        results = self.collection.query (
            query_embeddings=[query_embedding.tolist()],
            n_results=query_limit,
            where=where_filter,
            include=["metadatas", "distances", "documents"],
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
        

    #def load_sqlite_to_chroma(self, checkpoint_date: str|None=None, rebuild:bool = False):
    def load_sqlite_to_chroma(self, valid_products:list):
        """
        If rebuild then reads all products from an SQLite database, generates new embeddings,
        and completely rebuilds the ChromaDB collection. Otherwise, only update the Chroma
        database with products that are newer than the checkpoint_date
        """
                
        texts_to_embed = [p["embedding_text"] for p in valid_products]
        ids_to_upsert = [str(p["sku"]) for p in valid_products]
        metadatas_to_upsert: list[dict[str, str | int | float | bool | None]] = [
            self._clean_metadata(p) for p in valid_products
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
                f"Upserted {len(ids_to_upsert)} documents into ChromaDB collection '{self.collection_name}'."
            )
        except Exception as e:
            print(f"An error occurred while publishing to ChromaDB: {e}")
            return False

        return True
    
    def get_db_stats(self):
        """
        Gathers and returns statistics and histograms for all key filterable fields.
        """

        total_docs = self.collection.count()
        if total_docs == 0:
            return {"total_docs": 0, "last_update": "N/A", "histograms": {}}

        all_metadatas = self.collection.get(include=["metadatas"])["metadatas"]

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
                        item_list = []
                        if isinstance(json_string, str):
                            item_list = json_string.split(",")
                        elif isinstance(json_string, list):
                            item_list = json.loads(json_string)

                        item_list=[x.strip() for x in item_list if x]
                        #print (f"CHECK {item_list}")
                        counter.update(item_list)
                    except (json.JSONDecodeError, TypeError):   
                        print (f"MALFORMED data {type(json_string)} {json_string}")
                        pass  # Ignore malformed data

            parse_and_update_counter("tags", tag_counter)
            parse_and_update_counter("artist", artist_counter)
            parse_and_update_counter("compatibility", figure_counter)


        # We need to reduce the tag list because it may be very long when it contains small counts
        threshold = 10
        filtered_dict = {
            item: count 
            for item, count in tag_counter.items() 
            if count >= threshold
        }
        
        tag_counter = Counter(filtered_dict)
        
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

if __name__ == "__main__":
    from utilities import CHROMA_DB_PATH, COLLECTION_NAME
    db = ChromaDbManager(CHROMA_DB_PATH, COLLECTION_NAME)
    db.load_sqlite_to_chroma()
