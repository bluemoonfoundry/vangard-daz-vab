import json
import os
from datetime import datetime, timezone

from utilities import get_checkpoint, set_checkpoint, DAZ_EXTRACTED_PRODUCT_FILE
from scraper_process import run_scraper

from managers.managers import chroma_db_manager


def run_fetch_process():
    print("Simulating external fetch process...")
    # TODO: Replace with your actual library call.
    return True

def run_update_flow(task_status: dict):
    try:
        task_status.update(
            {
                "status": "running",
                "stage": "fetch",
                "progress": "Starting external fetch...",
            }
        )
        if not run_fetch_process():
            raise RuntimeError("Fetch process failed.")
        task_status["progress"] = "Fetch complete."

        task_status["stage"] = "scrape"
        try:
            with open(DAZ_EXTRACTED_PRODUCT_FILE, "r") as f:
                products = json.load(f)
        except FileNotFoundError:
            raise RuntimeError(f"Product file {DAZ_EXTRACTED_PRODUCT_FILE} not found.")

        checkpoint = get_checkpoint()
        urls_to_scrape = [
            p["url"]
            for p in products
            if p.get("date_installed", "1970-01-01T00:00:00Z") > checkpoint
            and p.get("url")
        ]

        if urls_to_scrape:
            task_status["progress"] = (
                f"Found {len(urls_to_scrape)} new products to scrape."
            )
            run_scraper(urls_to_scrape)
        task_status["progress"] = "Scraping complete."

        task_status["stage"] = "load"
        task_status["progress"] = "Loading new data into ChromaDB..."
        chroma_db_manager.load_sqlite_to_chroma(checkpoint, rebuild=False)
        set_checkpoint()
        task_status["progress"] = "Load complete."

        task_status.update(
            {
                "status": "complete",
                "stage": "finished",
                "progress": "Update process finished successfully.",
            }
        )
    except Exception as e:
        task_status.update({"status": "failed", "progress": f"An error occurred: {e}"})
