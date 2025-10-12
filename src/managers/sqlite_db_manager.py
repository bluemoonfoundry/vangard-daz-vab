import os
import sqlite3
import json
import logging
from datetime import datetime, timezone

class SQLiteWrapper:
    def __init__(self, sqlite_db_path, sqlite_db_table):
        self._logger = logging.getLogger(__name__)
        self.sqlite_db_path     = sqlite_db_path
        self.sqlite_db_table    = sqlite_db_table

    def setup_sqlite_db(self, force_reset:bool=False):
        """Creates the SQLite database and table using the final schema."""

        if force_reset and os.path.exists(self.sqlite_db_path):
            print("--force flag detected. Deleting existing SQLite database. {force_reset} {os.path.exists(self.sqlite_db_path)}")
            os.remove(self.sqlite_db_path)

        conn = self.get_connection()
        cursor = conn.cursor()
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.sqlite_db_table} (
                sku TEXT PRIMARY KEY, url TEXT, image_url TEXT, store TEXT, name TEXT, artist TEXT, price TEXT, description TEXT,
                tags TEXT, formats TEXT, poly_count TEXT, textures_info TEXT, required_products TEXT, compatible_figures TEXT,
                compatible_software TEXT, embedding_text TEXT, last_updated TEXT, category TEXT, subcategories TEXT, styles TEXT,
                inferred_tags TEXT, enriched_at TEXT, mature INTEGER
            )
        ''')
        conn.commit()
        conn.close()
        print(f"SQLite database '{self.sqlite_db_path}' and table '{self.sqlite_db_table}' are ready.")

    def get_connection(self):
        try:
            self.connection = sqlite3.connect(self.sqlite_db_path)
            self.connection.row_factory = sqlite3.Row
            #print (f"Connected to SQLite database at {self.sqlite_db_path}")
            #print (f"SQLite DB Table = {self.sqlite_db_table}")
        except sqlite3.OperationalError as e:
            self._logger.error(f"Error connecting to SQLite: {e}")
            raise e
        return self.connection

    def get_all_skus_from_sqlite(self):
        return self._fetchall_query(f"SELECT sku FROM {self.sqlite_db_table}")
    
    def get_content_by_sku_batch(self, sku_batch):
        conn = self.get_connection()
        cursor = conn.cursor()

        placeholders = ','.join(['?'] * len(sku_batch))
        
        sqlite_query = f"""
            SELECT sku, url, image_url, embedding_text, name, artist, compatible_figures, tags, category, subcategories
            FROM {self.sqlite_db_table}
            WHERE sku IN ({placeholders})
        """
        
        cursor.execute(sqlite_query, sku_batch)
        rows_to_embed = cursor.fetchall()
        conn.close()

        return rows_to_embed

    def _fetchall_query(self, query) -> list:
        """Efficiently fetches all existing SKUs from the SQLite database."""
        # if not os.path.exists(db_path):
        #     return []
        conn = self.get_connection()
        cursor = conn.cursor()
        try:
            cursor.execute(query)
            # fetchall returns a list of tuples, e.g., [('sku1',), ('sku2',)]
            results = [row[0] for row in cursor.fetchall()]
        except sqlite3.OperationalError as e:
            print(f"Error querying SQLite: {e}. The table might not exist yet.")
            results = []
        finally:
            conn.close()
        return results

    def execute_fetchone_query(self, query:str):
        content=None
        try:
            cursor = self.connection.cursor()
            cursor.execute (query)
            content = cursor.fetchone()[0]
        except sqlite3.OperationalError as e:
            print(f"Error reading from SQLite: {e}")

        return content

    def execute_fetchall_query(self, query:str) -> list:
        content=[]
        try:
            cursor = self.connection.cursor()
            cursor.execute (query)
            content = cursor.fetchall()
        except sqlite3.OperationalError as e:
            print(f"Error reading from SQLite: {e}")
        return content
    
    def count(self) -> int:
        q = f"SELECT count(*) from {self.sqlite_db_table}"
        rv = self.execute_fetchone_query(q)
        if rv is None:
            return -1
        else:
            return int(rv)

    def get_sku_row(self, sku) -> dict|None:
        q = f"SELECT * from {self.sqlite_db_table} where sku = '{sku}'"
        content = self.execute_fetchall_query (q)
        if len(content) > 0:
            return content[0]
        else:
            return None
        
    def get_post_checkpoint_rows(self, columns, checkpoint:str) -> list[dict]:
        content=[]
        q = f"""
        select {columns}
        from {self.sqlite_db_table}
        where Datetime(last_updated) > Datetime("{checkpoint}")
        """

        print (f"QUERY = [{q}]")
        rows = self.execute_fetchall_query (q)
        return  [dict(zip(row.keys(), row)) for row in rows]
        
    def insert_item(self, item):
        # Convert any list fields to JSON strings for database storage
        for key, value in item.items():
            if isinstance(value, list):
                item[key] = json.dumps(value)

        # Add the update timestamp
        item["last_updated"] = datetime.now(timezone.utc).isoformat()

        # Prepare columns and placeholders for upsert
        columns = ", ".join(item.keys())
        placeholders = ", ".join(["?"] * len(item))
        table = self.sqlite_db_table
        sql = f"INSERT OR REPLACE INTO {table} ({columns}) VALUES ({placeholders})"

        try:
            cursor = self.connection.cursor()
            cursor.execute(sql, list(item.values()))
            self.connection.commit()
            return True
        except Exception as e:
            print (
                f'Error: SQLite exception: {e}'
            )
            return False
        
    def close(self):
        self.connection.close()

if __name__ == '__main__':

    from managers import sqlite_db
    checkpoint = "2025-09-29T02:27:07.867181+00:00"

    #rv = sqlite_db.count()
    rv = sqlite_db.get_post_checkpoint_rows(checkpoint)
    print (f"SQLM: RV={rv}")
