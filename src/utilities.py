import os
import pathlib
import subprocess
import sys

import requests


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

        # # Construct the script args
        # script_args_parts = []
        # for (i, arg) in enumerate(script_args):
        #     script_args_parts.append (f"-scriptArg '{arg}'")

        # script_args_complete = " ".join(script_args_parts)

        # print(f"Executing DAZ Studio script: {script_file} {script_args_complete}")



        # command_expanded = f"\"{daz_root}\" {script_args_complete} {script_file}"

        #process = subprocess.Popen(command_expanded, shell=False)
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
