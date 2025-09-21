# src/enrich_data.py

import os
import json
import sqlite3
from datetime import datetime, timezone
from dotenv import load_dotenv

# Pydantic is used to define our desired JSON structure
from pydantic import BaseModel, Field
from typing import List, Literal

# --- LLM & AI Imports ---
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig
import outlines
from outlines import from_transformers

# --- 1. Define the Strict Output Schema ---
# This Pydantic model is the "contract" for what we expect from the LLM.
class ProductEnrichment(BaseModel):
    category: Literal[
        "Character", "Clothing", "Environment", "Props", 
        "Hair", "Poses", "Vehicle", "Other"
    ] = Field(description="The single most accurate primary product category.")
    
    subcategories: List[str] = Field(description="A list of specific subcategories. For Clothing, examples could be 'Dress', 'Sci-Fi Armor', 'Full Outfit'. For Character, 'Female', 'Male', 'Creature'.")
    
    styles: List[str] = Field(description="A list of descriptive styles or genres. Examples: 'Sci-Fi', 'Fantasy', 'Cyberpunk', 'Gothic', 'Contemporary', 'Historical', 'Steampunk', 'Photo-realistic'.")
    
    inferred_tags: List[str] = Field(description="A list of 5-10 additional useful, specific keywords inferred from the description that are not styles or categories. Examples: 'leather', 'weapon', 'game-ready', 'futuristic', 'high-detail'.")

# --- 2. Create the Local LLM Enricher Class ---
class LocalTransformerEnricher:
    """
    Handles loading a quantized Hugging Face model and running enrichment
    with guaranteed JSON output using the 'outlines' library.
    """
    def __init__(self, model_name: str):
        print(f"Initializing local transformer enricher with model: {model_name}")
        
        # --- 4-bit Quantization Configuration ---
        # This is CRUCIAL for loading large models like Gemma 2 9B on consumer GPUs.
        quantization_config = BitsAndBytesConfig(
            load_in_4bit=True,
            bnb_4bit_compute_dtype=torch.bfloat16
        )
        
        print("Loading quantized model (this may take a moment and download on first run)...")
        # Load the model with quantization and let accelerate handle device mapping
        model = AutoModelForCausalLM.from_pretrained(
            model_name,
            quantization_config=quantization_config,
            device_map="auto"
        )
        
        self.original_tokenizer = AutoTokenizer.from_pretrained(model_name)
        
        # Create an 'outlines' adapter to control the model's output
        self.model = outlines.from_transformers (model, self.original_tokenizer)
        print("Transformer model loaded successfully.")

    def enrich(self, product_info: dict) -> ProductEnrichment:
        """Generates structured data for a single product."""
        
        prompt = f"""
        You are an expert e-commerce cataloger for a 3D asset store.
        Analyze the following product details and categorize it by generating a JSON object that strictly follows the provided schema.

        **Product Information:**
        - Name: {product_info.get('name')}
        - Existing Tags: {product_info.get('tags')}
        - Description: {product_info.get('description')}
        """
        
        # Use the model's chat template for correct formatting
        messages = [{"role": "user", "content": prompt}]
        input_text = self.original_tokenizer.apply_chat_template(
            messages, 
            tokenize=False, 
            add_generation_prompt=True
        )
        
        # Use 'outlines' to force the model's output into our Pydantic schema
        # This eliminates JSON errors and ensures consistency.
        
        structured_output = self.model(input_text, ProductEnrichment, max_new_tokens=1024)
        #structured_output = utlines.generate.json(self.model, ProductEnrichment)(input_text)
        
        print (f"{type(structured_output)} = {structured_output}")
        #return structured_output.to_dict()
    
    def close(self):
        # Transformers models loaded this way don't need an explicit close()
        print("LocalTransformerEnricher closing.")
        pass

# --- 3. Main Script Logic ---
load_dotenv()
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "local")

def main(args):
    """Main loop to connect to the DB and process unenriched products."""
    
    enricher = None
    try:
        if LLM_PROVIDER == "local":
            model_name = os.getenv("LOCAL_TRANSFORMER_MODEL_NAME", "google/gemma-2-9b-it")
            enricher = LocalTransformerEnricher(model_name=model_name)
        else:
            raise NotImplementedError(f"Provider '{LLM_PROVIDER}' is not implemented yet.")

        conn = sqlite3.connect(SQLITE_DB_PATH)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()

        # Select products that haven't been enriched yet
        cursor.execute("SELECT * FROM product WHERE enriched_at IS NULL")
        products_to_process = cursor.fetchall()
        
        print(f"\nFound {len(products_to_process)} products to enrich.")
        
        for row in products_to_process:
            product = dict(row)
            sku = product.get('sku')
            print(f"--- Processing SKU: {sku} ({product.get('name')}) ---")
            
            try:
                enriched_data = enricher.enrich(product)
                
                # Update the database with the new structured data
                cursor.execute(
                    """UPDATE product
                       SET category = ?, subcategories = ?, styles = ?, inferred_tags = ?, enriched_at = ?
                       WHERE sku = ?""",
                    (
                        enriched_data.category,
                        json.dumps(enriched_data.subcategories),
                        json.dumps(enriched_data.styles),
                        json.dumps(enriched_data.inferred_tags),
                        datetime.now(timezone.utc).isoformat(),
                        sku
                    )
                )
                conn.commit()
                print(f"-> Success: Enriched and updated SKU {sku}.")
            except Exception as e:
                print(f"-> ERROR processing SKU {sku}: {e}")
        
        conn.close()
        print("\nEnrichment process complete.")
    finally:
        if enricher and hasattr(enricher, 'close'):
            enricher.close()

if __name__ == "__main__":
    main(args=None)