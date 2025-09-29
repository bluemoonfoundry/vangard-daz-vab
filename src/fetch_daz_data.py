import json
import os
import pathlib
from utilities import fetch_json_from_url, run_daz_script, get_checkpoint, set_checkpoint
from dotenv import load_dotenv

load_dotenv()

script_directory = pathlib.Path(__file__).parent.resolve()
product_file = os.getenv("DAZ_PRODUCT_PATH", f"{script_directory}/products.json")


def pre_fetch_faz_data(args):

    script_name = "ListProductsMetadataSA.dsa"
    script_args = [product_file, get_checkpoint()]
    rv = run_daz_script(script_name, script_args)
    if not rv:
        print("Error: DAZ script execution failed.")
        return False
    else:
        if not os.path.exists(product_file):
            print(f"Error: Expected output file '{product_file}' not found.")
            return False
        print(f"Wrote product metadata to {product_file}")
        set_checkpoint()
    return True

def fetch_daz_data(args):
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
    return True

if __name__ == "__main__":
    print("Direct execution of fetch_daz_data.py is not supported. Please run via main.py")