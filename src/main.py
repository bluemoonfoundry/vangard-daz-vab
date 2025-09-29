import argparse
import json
import os
import re
from datetime import datetime, timedelta, timezone
from output_formatters import print_pretty, print_json, print_table
from collections import Counter
import uvicorn
from dotenv import load_dotenv

from utilities import get_checkpoint, set_checkpoint
from fetch_daz_data import pre_fetch_faz_data, fetch_daz_data
from fetch_daz_data import product_file
from query_utils import get_db_stats, search
from rebuild_chroma import load_sqlite_to_chroma
from scraper_process import run_scraper

load_dotenv()

def pre_fetch_command(args):
    print("Starting prefetch command...")
    rv = pre_fetch_faz_data(args)
    print("Prefetch command complete.")
    return rv

def fetch_command(args):
    return fetch_daz_data(args)

def scrape_command(args):
    print("Starting scrape command...")
    dbfile = product_file

    try:
        with open(dbfile, "r") as f:
            products = json.load(f)
            print(f"Loaded {len(products)} products from {dbfile}.")
    except FileNotFoundError:
        print("Error: products.json not found. Run the 'fetch' command first.")
        return

    # --- THIS IS THE KEY CHANGE ---
    # We now build a list of dictionaries, not just URLs.
    products_to_scrape = []
    if args.update:
        checkpoint = get_checkpoint()
        print(f"Update mode enabled. Scraping products installed after {checkpoint}.")
        for product in products:
            if product.get("date_installed", "1970-01-01T00:00:00Z") > checkpoint:
                if product.get("url") and product.get("sku"):
                    products_to_scrape.append(
                        {
                            "url": product["url"],
                            "image_url": product["image_url"],
                            "sku": product["sku"],
                            "categoriesData": product.get("categoriesData", []),
                            "figureData": product.get("figureData", []),
                            "mature": product.get("mature", False),
                        }
                    )
    else:
        print("Update mode off. Scraping all products from products.json.")
        products_to_scrape = [
            {
                "url": p["url"],
                "image_url": p["image_url"],
                "sku": p["sku"],
                "categoriesData": p.get("categoriesData", []),
                "figureData": p.get("figureData", []),
                "mature": p.get("mature", False),
            }
            for p in products
            if p.get("url") and p.get("sku")
        ]

    if args.limit and args.limit > 0:
        print(
            f"Applying limit: processing only the first {args.limit} of {len(products_to_scrape)} products."
        )
        products_to_scrape = products_to_scrape[: args.limit]

    if products_to_scrape:
        # Pass the entire list of product dicts
        run_scraper(products_to_scrape)
    else:
        print("No new products to scrape based on the determined criteria.")


def enrich_command(args):
    #print("Starting LLM data enrichment process...")
    #run_enrichment(args)
    print ("Enrichment is not currently supported!")


def rebuild_command(args):
    """
    Performs a full, destructive rebuild of the ChromaDB collection
    from the SQLite database.
    """

    # Add a confirmation prompt to prevent accidental data loss
    confirm = input(
        "WARNING: This will delete and rebuild the entire ChromaDB collection. "
        "This can take a long time. Are you sure you want to continue? (yes/no): "
    )
    if confirm.lower() == "yes":
        print("Starting full ChromaDB rebuild...")
        load_sqlite_to_chroma(checkpoint_date=None, rebuild=True)
    else:
        print("Rebuild cancelled.")
    set_checkpoint()

def load_command(args):
    print("Starting load command...")
    checkpoint_date = get_checkpoint()
    load_sqlite_to_chroma(checkpoint_date, rebuild=False) # type: ignore
    set_checkpoint()


def query_command(args):
    """Submits a query to the ChromaDB and prints the formatted results."""
    print("Starting query command...")

    # The search function returns a dictionary with 'total_hits', 'results', etc.
    response = search(
        prompt=args.prompt,
        tags=args.tags,
        limit=args.limit,
        score_threshold=args.score,
        sort_by=args.sort_by,
        sort_order=args.sort_order,
    )

    if not response or not response.get("results"):
        print("\n--- No results found for your query. ---")
        return
    
    if args.format == 'json':
        print_json(response)
    elif args.format == 'table':
        print_table(response)
    else: # Default to 'pretty'
        print_pretty(response)

def stats_command(args):
    print("Gathering statistics from the database...")
    stats = get_db_stats()
    if stats is None:
        return
    
    #print (json.dumps(stats, indent=2))

    print("\n--- ChromaDB Collection Stats ---")
    print(f"Total Documents Indexed: {stats['total_docs']}")
    print(f"Last Document Update:    {stats['last_update']}")
    print("---------------------------------")
    
    if stats["histograms"]:
        
        for key in stats["histograms"]:
            #print (f"MARK {key} = {stats['histograms'][key]}")
            histogram = list(stats["histograms"][key])
            llen = len(histogram)
            maxlen = llen
            if llen > 0:
                if llen > 25:
                    llen=25

                print (f"\n -- Category {key} {llen} of {maxlen}")
                for tag, count in stats["histograms"][key].most_common(llen):
                    print(f"{tag:<30} | {count}")
                print("-------------------------------")
    else:
        print("\nNo tag information found.")


def server_command(args):
    if args.demo:
        print(
            "=" * 50
            + "\n==         STARTING SERVER IN DEMO MODE         ==\n"
            + "=" * 50
        )
        os.environ["APP_MODE"] = "demo"
    else:
        print("--- Starting server in Production Mode ---")
        os.environ["APP_MODE"] = "production"
    uvicorn.run("server:app", host=args.host, port=args.port, reload=True)


def openproduct_command(args):
    print("Starting openproduct command...")
    from open_daz_product import main as open_daz_product

    open_daz_product(args)


def main():
    parser = argparse.ArgumentParser(
        description="Visual Asset Browser Data Pipeline CLI"
    )
    # parser.add_argument(
    #     "--product-file",
    #     type=str,
    #     default="./products.json",
    #     help="Path to JSON file containing product data.",
    # )

    parser.add_argument(
        "--update",
        action="store_true",
        help="Only apply command to products that are not processed previously",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Define commands
    parsers = {
        "prefetch": subparsers.add_parser(
            "prefetch", help="Calls external library to create/update products.json."
        ),
        "fetch": subparsers.add_parser(
            "fetch", help="Loads external content from product pages for products in products.json"
        ),
        "scrape": subparsers.add_parser(
            "scrape", help="Scrape products and save to SQLite."
        ),
        "enrich": subparsers.add_parser(
            "enrich", help="Use an LLM to enrich product data."
        ),
        "rebuild": subparsers.add_parser(
            "rebuild", help="Nuke and rebuild the entire ChromaDB from SQLite."
        ),
        "load": subparsers.add_parser(
            "load", help="Load new data from SQLite into ChromaDB."
        ),
        "query": subparsers.add_parser(
            "query", help="Query the ChromaDB vector store."
        ),
        "stats": subparsers.add_parser(
            "stats", help="Display stats about the ChromaDB collection."
        ),
        "server": subparsers.add_parser("server", help="Run the FastAPI web server."),
        "openproduct": subparsers.add_parser(
            "openproduct",
            help="Open a naed DAZ product in DAZ Studio's Content Library Pane",
        ),
    }

    # Add arguments
    # parsers["scrape"].add_argument(
    #     "--update",
    #     action="store_true",
    #     help="Only scrape products newer than the last checkpoint.",
    # )
    parsers["scrape"].add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of URLs to process from the list.",
    )

    parsers["query"].add_argument("prompt", help="The search prompt.")
    parsers["query"].add_argument(
        "--tags", nargs="*", help="List of tags to pre-filter results."
    )
    parsers["query"].add_argument(
        "--limit", type=int, default=5, help="Max number of results."
    )
    parsers["query"].add_argument(
        "--score", type=float, default=1.0, help="Maximum distance (lower is better)."
    )
    parsers["query"].add_argument(
        "--sort-by", default="relevance", help="Field to sort by ('relevance', 'name')."
    )
    parsers["query"].add_argument(
        "--sort-order", choices=["ascending", "descending"], default="descending"
    )
    parsers["query"].add_argument("--categories", type=str, default=None)
    parsers["query"].add_argument(
        "--format",
        choices=['pretty', 'json', 'table'],
        default='pretty',
        help="The output format for the results."
    )
    parsers["query"].add_argument("--artists", nargs='*', help="List of artists to filter by.")
    parsers["query"].add_argument("--compatible_figures", nargs='*', help="List of figures to filter by.")

    parsers["server"].add_argument("--host", default="127.0.0.1")
    parsers["server"].add_argument("--port", type=int, default=8000)
    parsers["server"].add_argument(
        "--demo", action="store_true", help="Run server in demo mode."
    )
    

    parsers["openproduct"].add_argument(
        "--product", help="The name of the product to open in DAZ Studio."
    )

    # Set default functions
    func_map = {
        "prefetch": pre_fetch_command,
        "fetch": fetch_command,
        "scrape": scrape_command,
        "enrich": enrich_command,
        "rebuild": rebuild_command,
        "load": load_command,
        "query": query_command,
        "stats": stats_command,
        "server": server_command,
        "openproduct": openproduct_command,
    }
    for cmd, sub_parser in parsers.items():
        sub_parser.set_defaults(func=func_map[cmd])

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
