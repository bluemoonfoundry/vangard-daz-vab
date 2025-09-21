import json
import os
import pathlib
import subprocess
from utilities import run_daz_script
from dotenv import load_dotenv

load_dotenv()

script_directory = pathlib.Path(__file__).parent.resolve()
product_file = os.getenv("DAZ_PRODUCT_PATH", f"{script_directory}/products.json")


def main(args):

    script_name = "ListProductsMetadataSA.dsa"
    script_args = [product_file]
    rv = run_daz_script(script_name, script_args)
    if not rv:
        print("Error: DAZ script execution failed.")
        return False
    else:
        if not os.path.exists(product_file):
            print(f"Error: Expected output file '{product_file}' not found.")
            return False
        print(f"Wrote product metadata to {product_file}")

    return True
        
if __name__ == "__main__":
    main(args=None)
