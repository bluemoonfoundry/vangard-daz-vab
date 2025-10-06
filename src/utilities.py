import os
import pathlib
import subprocess
import sys
import datetime
from datetime import timedelta, timezone
from datetime import datetime
import requests
import operator
import requests
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright

from dotenv import load_dotenv
load_dotenv()

script_directory = pathlib.Path(__file__).parent.resolve()
CHECKPOINT_FILE = os.getenv("CHECKPOINT_FILE", ".checkpoint")
SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "products.db")
DAZ_EXTRACTED_PRODUCT_TABLE = "product"
CHROMA_DB_PATH = os.getenv("CHROMA_PATH", "db")
COLLECTION_NAME = os.getenv("CHROMA_COLLECTION", "daz_products")
DAZ_EXTRACTED_PRODUCT_FILE = os.getenv("DAZ_PRODUCT_PATH", f"{script_directory}/products.json")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", "mixedbread-ai/mxbai-embed-large-v1")

def get_checkpoint() -> str:
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, "r") as f:
            return f.read().strip()
    return "1970-01-01T00:00:00Z"

def set_checkpoint():
    with open(CHECKPOINT_FILE, "w") as f:
        f.write(datetime.now(timezone.utc).isoformat())
    print("Checkpoint updated.")

def compare_datetime_strings(d1:str, d2:str, op:str) -> bool:
    """
    Compares two datetime strings using a given comparison operator.

    Args:
        d1 (str): The first datetime string.
        d2 (str): The second datetime string.
        op (str): The comparison operator as a string: '>', '<', '==', '>=', '<=', '!='.

    Returns:
        bool: The result of the comparison (e.g., True if dt_str1 > dt_str2).

    Raises:
        ValueError: If the datetime strings cannot be parsed with the given format
                    or if the operator is invalid.
    """
    # Map operator strings to their corresponding functions from the operator module
    ops = {
        '>': operator.gt,
        '<': operator.lt,
        '==': operator.eq,
        '>=': operator.ge,
        '<=': operator.le,
        '!=': operator.ne
    }

    if op not in ops:
        raise ValueError(f"Invalid comparison operator: '{op}'. Use one of {list(ops.keys())}")

    # Try to parse the datetime strings into datetime objects
    try:
        dt1 = datetime.fromisoformat(d1)
        dt2 = datetime.fromisoformat(d2)
    except ValueError as e:
        # Re-raise with a more informative message
        raise ValueError(f"Could not parse datetime string. Ensure it matches the ISO format'. Original error: {e}")

    # Get the correct operator function from the dictionary and apply it
    return ops[op](dt1, dt2)


def run_daz_script(script_name: str, script_args:list) -> bool:
    """
    Executes a DAZ Studio script using the DAZ command-line interface.

    Args:
        script_path (str): The file path to the DAZ script to be executed.

    Returns:
        bool: True if the script executed successfully, False otherwise.
    """
    try:
        # Find the DAZ Studio executable
        daz_root = os.getenv("DAZ_STUDIO_EXE_PATH")
        if not daz_root or not os.path.exists(daz_root):
            print("Error: DAZ_STUDIO_EXE_PATH is not set correctly in the environment")
            return False
        
        # Find the script and make sure it exists
        script_directory = pathlib.Path(__file__).parent.resolve()
        script_file = f"{script_directory}/{script_name}"
        if not os.path.exists(script_file):
            print(f"Error: DAZ script '{script_file}' not found.")
            return False    

        command_list = [
            daz_root
        ]

        for arg in script_args:
            command_list.append("-scriptArg")
            command_list.append(arg)

        command_list.append(script_file)

        process = subprocess.Popen(
                command_list,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True, # Use text=True for string output, or omit for bytes
                shell=False 
            )
                

        process.wait()

        return True
    
    except Exception as e:
        print(f"An unexpected error occurred while executing the DAZ script: {e}", file=sys.stderr)
        return False
    


def fetch_playright_html_content(url: str) -> str | None:
    """
    Fetches the HTML content of a given URL using Playwright's synchronous API.
    This is the recommended way to use Playwright in a synchronous context.

    Args:
        url (str): The URL to fetch.

    Returns:
        str | None: The fully rendered HTML content as a string, or None if fetching fails.
    """
    # 'sync_playwright()' manages the Playwright driver and browser context synchronously

    with sync_playwright() as p:
        # Launch a headless browser (browser UI not shown)
        # You can choose 'chromium', 'firefox', or 'webkit'
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            print(f"Navigating to {url} with Playwright (sync API)...")
            # page.goto() is now a synchronous call
            page.goto(url, wait_until='networkidle') # Wait until network activity is minimal

            # Extract the fully rendered HTML content
            html_content = page.content()
            return html_content
        except Exception as e:
            print(f"An error occurred while fetching {url} with Playwright (sync API): {e}")
            return None
        finally:
            # Always close the browser
            browser.close()


def fetch_json_from_url(url: str, timeout: int = 10) -> dict | None:
    """
    Fetches content from a URL, parses it as JSON, and returns it.

    This function includes robust error handling for common network and data issues.

    Args:
        url (str): The URL to fetch data from.
        timeout (int): The number of seconds to wait for a server response.

    Returns:
        dict | None: A dictionary containing the parsed JSON data if successful,
                      otherwise None.
    """
    print(f"Fetching JSON from: {url}")
    try:
        # 1. Make the HTTP GET request with a timeout.
        response = requests.get(url, timeout=timeout)

        # 2. Check for HTTP errors (e.g., 404 Not Found, 500 Server Error).
        # This will raise an HTTPError if the status code is 4xx or 5xx.
        response.raise_for_status()

        # 3. Try to parse the response content as JSON.
        # This will raise a JSONDecodeError if the content is not valid JSON.
        return response.json()

    except requests.exceptions.HTTPError as http_err:
        # Handle specific HTTP status code errors (4xx/5xx).
        print(f"Error: HTTP Error occurred: {http_err}", file=sys.stderr)
        print(f"Status Code: {http_err.response.status_code}", file=sys.stderr)
        return None

    except requests.exceptions.JSONDecodeError:
        # Handle cases where the response is not valid JSON (e.g., it's HTML).
        print(
            f"Error: Failed to decode JSON. The content from the URL is not valid JSON.",
            file=sys.stderr,
        )
        return None

    except requests.exceptions.RequestException as req_err:
        # Handle broader network issues (e.g., DNS failure, connection refused, timeout).
        print(f"Error: A network error occurred: {req_err}", file=sys.stderr)
        return None

    except Exception as e:
        # A final catch-all for any other unexpected errors.
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return None


def extract_meta_tags(text):

    soup = BeautifulSoup(text,'lxml')

    meta_tags = soup.find_all('meta')

    tag_attributes = {}
   
    for tag in meta_tags:
        for attr in tag.attrs:
            if attr not in ["name", "content", "itemprop", "property","value","http-equiv"]:
                tag_attributes[attr] = tag.attrs[attr]
            elif attr not in["content","value"]:
                content = tag.get_attribute_list("content")[0]
                tag_attributes[tag.attrs[attr]] = content if content else tag.get_attribute_list("value")[0]

    return tag_attributes            

def fetch_html_content(url:str) -> tuple:
    html_content = None
    tag_attributes = None
    try:
    # Send a GET request to the URL
        response = requests.get(url)
        
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Get the HTML content of the page
            html_content = response.text
            tag_attributes = extract_meta_tags(html_content)
            #print("HTML content fetched successfully!")
            #print(html_content)  # Print the HTML content (optional)
        else:
            print(f"Failed to fetch the webpage. Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        # Handle exceptions (e.g., network issues, invalid URL)
        print(f"An error occurred: {e}")

    return html_content, tag_attributes


if __name__ == '__main__':
    d2 = "2025-09-29T02:27:07.867181+00:00"
    d1 = "2026-09-18T02:27:07.867181+00:00"

    dd2 = datetime.fromisoformat(d2)
    dd1 = datetime.fromisoformat(d1)

    print (dd1 > dd2)
    print (dd1 < dd2)

    print (compare_datetime_strings(d1, d2, '>'))
    print (compare_datetime_strings(d1, d2, '<'))    