import os
import sqlite3
import json
import logging
from datetime import datetime, timezone

class SQLiteWrapper:
    """A simple SQLite database manager for storing and retrieving product data."""

    def __init__(self, sqlite_db_path, sqlite_db_table):
        """Initializes the SQLiteWrapper with the database path and table name.
        
        Args:
            sqlite_db_path (str): Path to the SQLite database file. 
            sqlite_db_table (str): Name of the table to use within the SQLite database.
        """
        self._logger = logging.getLogger(__name__)
        self.sqlite_db_path     = sqlite_db_path
        self.sqlite_db_table    = sqlite_db_table

    def setup_sqlite_db(self, force_reset:bool=False):
        """Creates the SQLite database and table using the final schema.
        
        Args:
            force_reset (bool): If True, deletes any existing database file before creating a new one
        """

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
        """Establishes and returns a connection to the SQLite database."""


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
        """Fetches all existing SKUs from the SQLite database.
        
        Returns:
            list: A list of all SKUs in the SQLite database as strings.
        """
        return self._fetchall_query(f"SELECT sku FROM {self.sqlite_db_table}")
    
    def get_content_by_sku_batch(self, sku_batch):
        """Fetches full product data for a given batch of SKUs from SQLite.
        
        Args:
            sku_batch (list): A list of SKUs to fetch data for.

        Returns:    
            list: A list of dictionaries containing product data for the given SKUs.
        """
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
        """Helper method to execute a query and return all results as a list of SKUs.

        Args:
            query (str): The SQL query to execute.

        Returns:
            list: A list of SKUs as strings.
        """

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
        """Executes a query and returns a single result.

        Args:
            query (str): The SQL query to execute.  

        Returns:    
            any: The first column of the first row of the result, or None if no result.
        """

        content=None
        try:
            cursor = self.connection.cursor()
            cursor.execute (query)
            content = cursor.fetchone()[0]
        except sqlite3.OperationalError as e:
            print(f"Error reading from SQLite: {e}")

        return content

    def execute_fetchall_query(self, query:str) -> list:
        """Executes a query and returns all results.

        Args:
            query (str): The SQL query to execute.  

        Returns:    
            list: A list of all rows returned by the query.
        """
        content=[]
        try:
            cursor = self.connection.cursor()
            cursor.execute (query)
            content = cursor.fetchall()
        except sqlite3.OperationalError as e:
            print(f"Error reading from SQLite: {e}")
        return content
    
    def count(self) -> int:
        """Returns the total number of rows in the SQLite table."""
        q = f"SELECT count(*) from {self.sqlite_db_table}"
        rv = self.execute_fetchone_query(q)
        if rv is None:
            return -1
        else:
            return int(rv)

    def get_sku_row(self, sku) -> dict|None:
        """Fetches the full row for a given SKU from SQLite.
        
        Args:
            sku (str): The SKU to fetch data for.   

        Returns:    
            dict|None: A dictionary containing the product data for the given SKU, or None if not found.
        
        """
        q = f"SELECT * from {self.sqlite_db_table} where sku = '{sku}'"
        content = self.execute_fetchall_query (q)
        if len(content) > 0:
            return content[0]
        else:
            return None
        
    def get_post_checkpoint_rows(self, columns, checkpoint:str) -> list[dict]:
        """Fetches all rows updated after a given checkpoint timestamp.

        Args:
            columns (str): Comma-separated list of columns to retrieve.
            checkpoint (str): ISO 8601 formatted timestamp string.  

        Returns:
            list[dict]: A list of dictionaries representing the rows updated after the checkpoint.
        """

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
        """Inserts or updates a product item in the SQLite database.

        Args:
            item (dict): A dictionary containing the product data to insert or update.  
        """ 
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
        """Closes the SQLite database connection."""
        self.connection.close()

if __name__ == '__main__':

    from managers import sqlite_db
    checkpoint = "2025-09-29T02:27:07.867181+00:00"

    #rv = sqlite_db.count()
    rv = sqlite_db.get_post_checkpoint_rows(checkpoint)
    print (f"SQLM: RV={rv}")
