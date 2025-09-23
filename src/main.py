import argparse
import json
import os
import re
import sys
from datetime import datetime, timedelta, timezone
from output_formatters import print_pretty, print_json, print_table

import uvicorn
from dotenv import load_dotenv

from database_utils import load_sqlite_to_chroma
from enrich_data import main as run_enrichment
from fetch_daz_data import main as fetch_daz_data
from fetch_daz_data import product_file
from query_utils import get_db_stats, search
from rebuild_chroma import main as run_rebuild
from scraper_process import run_scraper
from utilities import fetch_json_from_url

load_dotenv()


def slugify_regex(text: str) -> str:
    """
    Downcases a string and replaces all whitespace sequences with a dash
    using regular expressions.

    Args:
        text: The input string to be processed.

    Returns:
        A slug version of the string.
    """
    # 1. Convert to lowercase
    lower_text = text.lower()

    # 2. Replace one or more whitespace characters (\s+) with a single dash
    slug = re.sub(r"\s+", "-", lower_text)

    # 3. (Optional) Remove leading/trailing dashes that might be created
    return slug.strip("-")


CHECKPOINT_FILE = ".checkpoint"


def get_checkpoint():
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return (datetime.now(timezone.utc) - timedelta(days=365 * 10)).isoformat()


def set_checkpoint():
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())
    print(f"Checkpoint updated to {get_checkpoint()}")


def fetch_command(args):
    print("Starting fetch command...")
    rv = fetch_daz_data(args)
    if rv and args.prefetch_only == False:

        # https://www.daz3d.com/cdn-cgi/image/width=380,height=494,fit=cover/https://gcdn.daz3d.com/p/90233/i/dforcefantasyholooutfitforgenesis9and8females00thumbdaz3d.jpg
        # If we got valid data, read it and get the correct product url for each product

        print(f"+++++++++++++++++ TEST {product_file}")
        product_data = json.load(open(product_file, "r"))
        print(f"Fetched {len(product_data)} products from DAZ Studio.")

        x = 0
        for product in product_data:
            if "url" not in product or not product["url"]:
                if product["store_id"] == 1:  # DAZ Store
                    sku = product.get("sku")
                    slab_url = f"http://www.daz3d.com/dazApi/slab/{sku}"
                    content = fetch_json_from_url(slab_url)
                    print(f"+++++ Process Item {x} of {len(product_data)}")
                    if content is not None:
                        image_root_url = content["imageUrl"]
                        image_url = image_root_url[
                            image_root_url.rfind("https://gcdn") :
                        ]

                        product_url = f"https://www.daz3d.com/{content['url']}"
                        product["url"] = product_url
                        product["image_url"] = image_url
                        product["mature"] = content.get("mature", False)
                        product["categoriesData"] = content.get("categoriesData", [])
                        product["figureData"] = content.get("figureData", [])
                        # Extract categoriesData and figureData if available
                        print(
                            f"Info: Computed DAZ Store URL for '{product['title']}' as '{product['url']}' with image_url '{image_url}'."
                        )
                else:
                    print(
                        f"Warning: No URL computable for '{product['title']}' (Store ID: {product['store_id']})."
                    )
            x += 1
        with open(product_file, "w") as f:
            json.dump(product_data, f, indent=2)
    print("Fetch command complete.")


def scrape_command(args):
    print("Starting scrape command...")
    dbfile = args.product_file

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
    print("Starting LLM data enrichment process...")
    run_enrichment(args)


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
        run_rebuild()
    else:
        print("Rebuild cancelled.")


def load_command(args):
    print("Starting load command...")
    checkpoint = get_checkpoint()
    load_sqlite_to_chroma(checkpoint) # type: ignore
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

    # # Print a summary header
    # total_hits = response.get("total_hits", 0)
    # returned_count = len(response["results"])
    # offset = response.get("offset", 0)

    # print("\n" + "=" * 60)
    # print(
    #     f"Showing results {offset + 1} - {offset + returned_count} of {total_hits} total matches."
    # )
    # print("=" * 60)

    # # Loop through and print each result
    # for i, res in enumerate(response["results"]):
    #     metadata = res.get("metadata", {})
    #     distance = res.get("distance", -1.0)

    #     print(f"\n--- Result #{offset + i + 1} ---")
    #     print(f"  Name:     {metadata.get('name', 'N/A')}")
    #     print(f"  Artist:   {metadata.get('artist', 'N/A')}")
    #     print(f"  Category: {metadata.get('category', 'N/A')}")
    #     print(f"  SKU:      {res.get('id', 'N/A')}")
    #     print(f"  URL:      {metadata.get('url', 'N/A')}")
    #     print(f"  Image:    {metadata.get('image_url', 'N/A')}")
    #     print(f"  Score (Distance): {distance:.4f} (lower is better)")

    #     # Optionally print tags if they exist
    #     if tags := metadata.get("tags"):
    #         try:
    #             # Tags are stored as a JSON string, so we parse and display them nicely
    #             tag_list = json.loads(tags)
    #             print(f"  Tags:     {', '.join(tag_list)}")
    #         except (json.JSONDecodeError, TypeError):
    #             # Fallback for non-JSON tag strings
    #             print(f"  Tags:     {tags}")

    # print("\n" + "=" * 60)


def stats_command(args):
    print("Gathering statistics from the database...")
    stats = get_db_stats()
    if stats is None:
        return

    print("\n--- ChromaDB Collection Stats ---")
    print(f"Total Documents Indexed: {stats['total_docs']}")
    print(f"Last Document Update:    {stats['last_update']}")
    print("---------------------------------")
    if stats["tag_histogram"]:
        print("\n--- Top 25 Most Common Tags ---")
        for tag, count in stats["tag_histogram"].most_common(25):
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
    parser.add_argument(
        "--product-file",
        type=str,
        default="./products.json",
        help="Path to JSON file containing product data.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Define commands
    parsers = {
        "fetch": subparsers.add_parser(
            "fetch", help="Calls external library to create/update products.json."
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
    parsers["scrape"].add_argument(
        "--update",
        action="store_true",
        help="Only scrape products newer than the last checkpoint.",
    )
    parsers["scrape"].add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of URLs to process from the list.",
    )

    parsers["fetch"].add_argument("--prefetch-only", action="store_true", default=False,
                                  help="Only fetch DAZ product metadata, do execute full database construction.")

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
