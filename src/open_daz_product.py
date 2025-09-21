import json
import os
import pathlib
import subprocess
from utilities import run_daz_script
from dotenv import load_dotenv

load_dotenv()

script_directory = pathlib.Path(__file__).parent.resolve()


def main(args):
    product_name = args.product if args and args.product else None

    if product_name is not None:
        print(f"Opening product '{product_name}' in DAZ Studio...")
        return run_daz_script("OpenProductInContentLibrarySA.dsa", [args.product])
    else:
        print("Error: No product name specified to open.")
        return False

if __name__ == "__main__":
    main(args=None)
