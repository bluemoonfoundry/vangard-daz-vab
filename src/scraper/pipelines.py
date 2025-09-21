# asset_scraper/pipelines.py

import json
import re
import sqlite3
from datetime import datetime, timezone


def clean_list_field(items):
    """Helper function to strip whitespace from a list of strings."""
    if not items:
        return []
    return [item.strip() for item in items if item.strip()]


class AssetProcessingPipeline:
    """
    This pipeline cleans raw scraped data and distills it into a single
    text field suitable for generating embeddings.
    """

    def process_item(self, item, spider):
        # --- DEBUG LOGGING ---
        spider.logger.info(
            f"[AssetProcessingPipeline] Received item with SKU: {item.get('sku')}"
        )
        # --- END DEBUG ---

        # --- 1. Data Cleaning ---
        if item.get("price"):
            item["price"] = re.sub(r"[^\d.]", "", item["price"])
        if item.get("poly_count"):
            poly_str = item.get("poly_count", "")
            item["poly_count"] = re.sub(r"\D", "", poly_str)
        if item.get("sku"):
            item["sku"] = item["sku"].strip()

        # Clean all list-based fields
        for field in [
            "artist",
            "tags",
            "formats",
            "required_products",
            "compatible_figures",
            "compatible_software",
        ]:
            if item.get(field):
                item[field] = clean_list_field(item[field])

        # --- 2. Create the Embedding Text ---
        parts = []
        if category := item.get("category"):
            parts.append(f"A 3D asset of category: {category}.")
        if item.get("mature"):
            parts.append("This is a mature product.")

        if name := item.get("name"):
            parts.append(f"3D model of a {name}.")
        if artist := item.get("artist"):
            parts.append(f"Created by {', '.join(artist)}.")
        if description := item.get("description"):
            parts.append(description)

        compat_parts = []
        if figs := item.get("compatible_figures"):
            compat_parts.append(f"compatible with figures like {', '.join(figs)}")
        if software := item.get("compatible_software"):
            compat_parts.append(f"works in software such as {', '.join(software)}")
        if compat_parts:
            parts.append(
                f"This asset is designed so that it is {' and '.join(compat_parts)}."
            )

        if req := item.get("required_products"):
            parts.append(f"Requires the following products: {', '.join(req)}.")
        if tags := item.get("tags"):
            parts.append(f"This asset is tagged with: {', '.join(tags)}.")
        if store := item.get("store"):
            if sku := item.get("sku"):
                parts.append(f"This is product SKU {sku} from the {store} store.")

        item["embedding_text"] = " ".join(parts).replace("  ", " ")
        return item


class SQLitePipeline:
    """
    This pipeline takes the processed item and saves it to an SQLite database.
    It uses INSERT OR REPLACE to handle updates to existing products.
    """

    def __init__(self, sqlite_db, sqlite_table):
        self.sqlite_db = sqlite_db
        self.sqlite_table = sqlite_table

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            sqlite_db=crawler.settings.get("SQLITE_DB", "products.db"),
            sqlite_table=crawler.settings.get("SQLITE_TABLE", "product"),
        )

    def open_spider(self, spider):
        self.conn = sqlite3.connect(self.sqlite_db)
        self.cursor = self.conn.cursor()
        # Create the table with all possible columns, including those for later enrichment.
        self.cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.sqlite_table} (
                sku TEXT PRIMARY KEY,
                url TEXT,
                image_url TEXT
                store TEXT,
                name TEXT,
                artist TEXT,
                price TEXT,
                description TEXT,
                tags TEXT,
                formats TEXT,
                poly_count TEXT,
                textures_info TEXT,
                required_products TEXT,
                compatible_figures TEXT,
                compatible_software TEXT,
                embedding_text TEXT,
                last_updated TEXT,
                -- Columns for LLM enrichment
                category TEXT,
                subcategories TEXT,
                styles TEXT,
                inferred_tags TEXT,
                enriched_at TEXT,
                mature INTEGER
            )
        """
        )
        self.conn.commit()

    def close_spider(self, spider):
        self.conn.close()

    def process_item(self, item, spider):
        # Convert any list fields to JSON strings for database storage
        for key, value in item.items():
            if isinstance(value, list):
                item[key] = json.dumps(value)

        # Add the update timestamp
        item["last_updated"] = datetime.now(timezone.utc).isoformat()

        # Prepare columns and placeholders for a robust upsert operation
        columns = ", ".join(item.keys())
        placeholders = ", ".join(["?"] * len(item))
        sql = f"INSERT OR REPLACE INTO {self.sqlite_table} ({columns}) VALUES ({placeholders})"

        try:
            self.cursor.execute(sql, list(item.values()))
            self.conn.commit()
            spider.logger.info(f"Successfully saved item {item.get('sku')} to SQLite.")
        except Exception as e:
            spider.logger.error(f"Failed to save item {item.get('sku')} to SQLite: {e}")

        return item
