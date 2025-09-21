import os, subprocess, json
import pathlib


from dotenv import load_dotenv
load_dotenv()

script_directory = pathlib.Path(__file__).parent.resolve()
product_file = os.getenv("DAZ_PRODUCT_PATH", f"{script_directory}/products.json")

def main(args):
    daz_root = os.getenv("DAZ_STUDIO_EXE_PATH")
    if not daz_root or not os.path.exists(daz_root):
        print("Error: DAZ_STUDIO_EXE_PATH is not set correctly in the environment")
        return False

    
    script_file = f"{script_directory}/ListProductsMetadataSA.dsa"

    print (f'Executing DAZ Studio script: {script_file}')

    command_expanded = f'"{daz_root}" -scriptArg \'{product_file}\' {script_file}'

    process = subprocess.Popen (command_expanded, shell=False)

    process.wait()
    
    if not os.path.exists(product_file):
        print(f"Error: Expected output file '{product_file}' not found.")
        return False
 
    print(f'Wrote product metadata to {product_file}')

    
    return True

if __name__ == "__main__":
    main(args=None)