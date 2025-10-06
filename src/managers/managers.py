from utilities import SQLITE_DB_PATH, DAZ_EXTRACTED_PRODUCT_TABLE, DAZ_EXTRACTED_PRODUCT_FILE, CHROMA_DB_PATH, COLLECTION_NAME
from .sqlite_db_manager import SQLiteWrapper
from .daz_db_manager import DazDbManager
from .chroma_db_manager import ChromaDbManager

sqlite_db = SQLiteWrapper(
    sqlite_db_path  = SQLITE_DB_PATH,
    sqlite_db_table = DAZ_EXTRACTED_PRODUCT_TABLE
)

daz_db_manager = DazDbManager(DAZ_EXTRACTED_PRODUCT_FILE, sqlite_db)
    
chroma_db_manager = ChromaDbManager(CHROMA_DB_PATH, COLLECTION_NAME, sqlite_db)