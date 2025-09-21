import requests
import sys

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
        print(f"Error: Failed to decode JSON. The content from the URL is not valid JSON.", file=sys.stderr)
        return None

    except requests.exceptions.RequestException as req_err:
        # Handle broader network issues (e.g., DNS failure, connection refused, timeout).
        print(f"Error: A network error occurred: {req_err}", file=sys.stderr)
        return None
        
    except Exception as e:
        # A final catch-all for any other unexpected errors.
        print(f"An unexpected error occurred: {e}", file=sys.stderr)
        return None