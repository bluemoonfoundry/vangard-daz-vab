import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv
load_dotenv()

class DazDBAnalyzer:
    """
    Connects to the Daz Content PostgreSQL database to extract product data.
    """
    QUERY_BODY = """
        SELECT
            p.id AS product_id,
            p.name AS product_name,
            p.artists,
            p.token AS sku,
            COALESCE(p.last_update, p.date_installed) AS last_modified_date,
            REGEXP_REPLACE(
                STRING_AGG(DISTINCT final_compat.compatibility_name, ', '),
                '^, *| *, $', ''
            ) AS product_compatibility,
            COUNT(DISTINCT c.id) AS content_item_count,
            STRING_AGG(DISTINCT cat."fldCategoryName", ', ') AS categories,
            STRING_AGG(DISTINCT ct."fldType", ', ') AS content_types
        FROM dzcontent.product AS p
        LEFT JOIN dzcontent.content AS c ON p.id = c.product_id
        LEFT JOIN (
            SELECT
                c_inner.id AS content_id,
                COALESCE(
                    cb."fldCompatibilityBase",
                    CASE
                        WHEN c_inner.path ILIKE '%%/Genesis 9/%%' OR c_inner.filename ILIKE '%%Genesis 9%%' OR c_inner.filename ILIKE '%%G9%%' THEN 'Genesis 9'
                        WHEN c_inner.path ILIKE '%%/Genesis 8.1/%%' OR c_inner.filename ILIKE '%%Genesis 8.1%%' OR c_inner.filename ILIKE '%%G8_1%%' THEN 'Genesis 8.1'
                        WHEN c_inner.path ILIKE '%%/Genesis 8/%%' OR c_inner.filename ILIKE '%%Genesis 8%%' OR c_inner.filename ILIKE '%%G8%%' THEN 'Genesis 8'
                        WHEN c_inner.path ILIKE '%%/Genesis 3/%%' OR c_inner.filename ILIKE '%%Genesis 3%%' OR c_inner.filename ILIKE '%%G3%%' THEN 'Genesis 3'
                        WHEN c_inner.path ILIKE '%%/Genesis 2/%%' OR c_inner.filename ILIKE '%%Genesis 2%%' OR c_inner.filename ILIKE '%%G2%%' THEN 'Genesis 2'
                        ELSE NULL
                    END
                ) AS compatibility_name
            FROM dzcontent.content AS c_inner
            LEFT JOIN dzcontent.compatibility_base_content AS cbc ON c_inner.id = cbc.content_id
            LEFT JOIN dzcontent."tblCompatibilityBase" AS cb ON cbc.compatibility_base_id = cb."RecID"
        ) AS final_compat ON c.id = final_compat.content_id
        LEFT JOIN dzcontent.category_content AS cc ON c.id = cc.content_id
        LEFT JOIN dzcontent."tblCategories" AS cat ON cc.category_id = cat."RecID"
        LEFT JOIN dzcontent."tblType" AS ct ON c.content_type_id = ct."RecID"
    """
    QUERY_GROUPING = "GROUP BY p.id, p.name, p.artists, p.token, p.last_update, p.date_installed ORDER BY p.name"

    def __init__(self):        
        try:
            db_config = {
                "dbname": os.environ['DB_NAME'], 
                "user": os.environ['DB_USER'], 
                "password": os.environ['DB_PASS'], 
                "host": os.environ['DB_HOST'], 
                "port": os.environ['DB_PORT']
            }
        except KeyError as e: 
            print(f"Error: Missing environment variable {e}.")
            return

        self.db_config = db_config
        self.batch_size = int(os.getenv("BATCH_SIZE", 512))

    def _execute_query(self, sql, params=None):
        results = []
        try:
            with psycopg2.connect(**self.db_config) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                    cur.execute(sql, params)
                    if cur.description:
                        results = cur.fetchall()
        except psycopg2.Error as e:
            print(f"PostgreSQL Database error: {e}")
            return None
        return [dict(row) for row in results]

    def get_all_skus(self):
        """
            Efficiently fetches a list of all non-null AND non-empty SKUs from PostgreSQL.
        """
        print("Fetching all valid SKUs from PostgreSQL...")
        # This query now filters out both NULL and empty strings
        sql = "SELECT token FROM dzcontent.product WHERE token IS NOT NULL AND token != ''"
        results = self._execute_query(sql)
        return [row['token'] for row in results] if results else []

    def get_products_by_sku_list(self, skus):
        """Fetches full product data for a given list of SKUs, handling batching."""
        if not skus:
            return []
        print(f"Fetching full data for {len(skus)} products from PostgreSQL...")
        all_products = []
        for i in range(0, len(skus), self.batch_size):
            sku_batch = skus[i:i + self.batch_size]
            placeholders = ','.join(['%s'] * len(sku_batch))
            where_clause = f"WHERE p.token IN ({placeholders})"
            sql = f"{self.QUERY_BODY} {where_clause} {self.QUERY_GROUPING}"
            batch_results = self._execute_query(sql, tuple(sku_batch))
            if batch_results:
                all_products.extend(batch_results)
        return all_products