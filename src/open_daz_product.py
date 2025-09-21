import os, subprocess, json
import pathlib


from dotenv import load_dotenv
load_dotenv()

script_directory = pathlib.Path(__file__).parent.resolve()

def main(args):
    daz_root = os.getenv("DAZ_STUDIO_EXE_PATH")
    if not daz_root or not os.path.exists(daz_root):
        print("Error: DAZ_STUDIO_EXE_PATH is not set correctly in the environment")
        return False

    
    script_file = f"{script_directory}/OpenProductInContentLibrarySA.dsa"
    product_name = args.product if args and args.product else None

    if product_name is not None:

        print (f'Executing DAZ Studio script: {script_file}')

        command_expanded = f'"{daz_root}" -scriptArg \'{product_name}\' {script_file}'

        process = subprocess.Popen (command_expanded, shell=False)

        process.wait()
        
    
    return True

if __name__ == "__main__":
    main(args=None)