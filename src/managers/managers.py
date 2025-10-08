import os
from .daz_db_analyzer import DazDBAnalyzer
from .sqlite_db_manager import SQLiteWrapper
from .chroma_db_manager import ChromaDbManager

from dotenv import load_dotenv
load_dotenv()

sqlite_db = SQLiteWrapper(
    sqlite_db_path  = os.environ.get("SQLITE_DB_PATH"),
    sqlite_db_table = os.environ.get("SQLITE_TABLE_NAME")
)

chroma_db_manager = ChromaDbManager(
    os.environ.get("CHROMA_PATH"), 
    os.environ.get("CHROMA_COLLECTION")
)

daz_pg_analyzer = DazDBAnalyzer()