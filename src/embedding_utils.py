# src/embedding_utils.py

import os
from sentence_transformers import SentenceTransformer
from dotenv import load_dotenv

# Load environment variables once
load_dotenv()

# --- Global Model Cache ---
# This ensures the model is only loaded into memory once per process.
_model = None

def get_embedding_model():
    """
    Loads and caches the embedding model specified in the .env file.
    This function acts as a singleton for the embedding model.
    """
    global _model
    if _model is None:
        # Default to the recommended model if not specified in .env
        model_name = os.getenv("EMBEDDING_MODEL_NAME", "mixedbread-ai/mxbai-embed-large-v1")
        print(f"--- Loading embedding model: {model_name} ---")
        # You can specify the device, e.g., device='cuda', if you have a GPU
        _model = SentenceTransformer(model_name)
        print("--- Embedding model loaded. ---")
    return _model

def generate_embeddings(texts, is_query: bool = False):
    """
    Generates embeddings for a given text or list of texts.
    Handles model-specific prefixes for query vs. passage.
    
    Args:
        texts (str or list[str]): The text(s) to embed.
        is_query (bool): True if the text is a search query, False if it's a document.

    Returns:
        numpy.ndarray: The embedding vector(s).
    """
    model = get_embedding_model()
    
    # Some models, like mxbai, recommend a specific prefix for queries
    # to improve retrieval performance.
    model_name = os.getenv("EMBEDDING_MODEL_NAME", "")

    print (f'+++ Using {model_name} for embedding +++')

    if "mxbai" in model_name and is_query:
        # Ensure that if a list is passed, we prefix each item
        if isinstance(texts, list):
            texts = [f"Represent this sentence for searching relevant passages: {text}" for text in texts]
        else:
            texts = f"Represent this sentence for searching relevant passages: {texts}"
    
    # The .encode() method handles batching automatically for lists
    return model.encode(texts, convert_to_tensor=False)