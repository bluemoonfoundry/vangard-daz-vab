import json
import os
import pathlib
from utilities import fetch_json_from_url, run_daz_script, get_checkpoint, set_checkpoint, DAZ_EXTRACTED_PRODUCT_FILE, sqlite_db
from dotenv import load_dotenv

load_dotenv()


def pre_fetch_faz_data(args):
    script_name = "ListProductsMetadataSA.dsa"
    script_args = [DAZ_EXTRACTED_PRODUCT_FILE]
    rv = run_daz_script(script_name, script_args)
    if not rv:
        print("Error: DAZ script execution failed.")
        return False
    else:
        if not os.path.exists(DAZ_EXTRACTED_PRODUCT_FILE):
            print(f"Error: Expected temp output file '{DAZ_EXTRACTED_PRODUCT_FILE}' not found.")
            return False
        
        # Now that we have the set of product data from DAZ, cycle through the list. Entries that 
        # do not have a url attribute set are assumed to be new products that we should splice into
        # the product file. 

        print(f"Wrote product metadata to {DAZ_EXTRACTED_PRODUCT_FILE}")
        set_checkpoint()


    return True

def fetch_daz_data(args):
    # https://www.daz3d.com/cdn-cgi/image/width=380,height=494,fit=cover/https://gcdn.daz3d.com/p/90233/i/dforcefantasyholooutfitforgenesis9and8females00thumbdaz3d.jpg
    # If we got valid data, read it and get the correct product url for each product

    print(f"+++++++++++++++++ TEST {DAZ_EXTRACTED_PRODUCT_FILE}")
    product_data = json.load(open(DAZ_EXTRACTED_PRODUCT_FILE, "r"))
    print(f"Fetched {len(product_data)} products from DAZ Studio.")

    # If we are operating in update mode, then we should only try to process content that doesn't 
    # have an entry in the SQLite database already


    x = 0
    for product in product_data:
        sku = product.get("sku")
        row = sqlite_db.get_sku_row(sku)

        # If the SKU isn't in the sqlite database yet, create a base entry for it
        # The problem is not definitely going to be that we have to keep any
        # attributes we create here in sync with the scraper pipeline that 
        # enters information during scraping. So let's make sure that we at a
        # minimum have an entry with a minimal set of columns
        #
        if row is None:
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

                    # We should be able to insert a row into the Sqlite database at this point, to create the base 
                    # information about a product 
                    
            else:
                print(
                    f"Warning: No URL computable for '{product['title']}' (Store ID: {product['store_id']})."
                )
        x += 1
    with open(DAZ_EXTRACTED_PRODUCT_FILE, "w") as f:
        json.dump(product_data, f, indent=2)        
    return True

if __name__ == "__main__":
    print("Direct execution of fetch_daz_data.py is not supported. Please run via main.py")