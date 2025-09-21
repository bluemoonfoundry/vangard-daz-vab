import json, os
from datetime import datetime, timezone
from scraper_process import run_scraper
from database_utils import load_sqlite_to_chroma

def run_fetch_process():
    print("Simulating external fetch process...")
    # TODO: Replace with your actual library call.
    return True

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "products.db")
CHECKPOINT_FILE = ".checkpoint"
def get_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, 'r') as f: return f.read().strip()
    return "1970-01-01T00:00:00Z"

def set_checkpoint():
    with open(CHECKPOINT_FILE, 'w') as f: f.write(datetime.now(timezone.utc).isoformat())
    print("Checkpoint updated.")

def run_update_flow(task_status: dict):
    try:
        task_status.update({'status': 'running', 'stage': 'fetch', 'progress': 'Starting external fetch...'})
        if not run_fetch_process(): raise RuntimeError("Fetch process failed.")
        task_status['progress'] = 'Fetch complete.'

        task_status['stage'] = 'scrape'
        try:
            with open("products.json", 'r') as f: products = json.load(f)
        except FileNotFoundError: raise RuntimeError("products.json not found.")

        checkpoint = get_checkpoint()
        urls_to_scrape = [p['url'] for p in products if p.get('date_installed', '1970-01-01T00:00:00Z') > checkpoint and p.get('url')]
        
        if urls_to_scrape:
            task_status['progress'] = f'Found {len(urls_to_scrape)} new products to scrape.'
            run_scraper(urls_to_scrape)
        task_status['progress'] = 'Scraping complete.'

        task_status['stage'] = 'load'
        task_status['progress'] = 'Loading new data into ChromaDB...'
        load_sqlite_to_chroma(SQLITE_DB_PATH, checkpoint)
        set_checkpoint()
        task_status['progress'] = 'Load complete.'

        task_status.update({'status': 'complete', 'stage': 'finished', 'progress': 'Update process finished successfully.'})
    except Exception as e:
        task_status.update({'status': 'failed', 'progress': f"An error occurred: {e}"})