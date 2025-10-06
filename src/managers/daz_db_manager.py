import json
import html
import xmltodict
import traceback
import os
import logging
from managers.sqlite_db_manager import SQLiteWrapper
from utilities import run_daz_script, fetch_json_from_url, fetch_html_content

from bs4 import BeautifulSoup, Tag


class DazDbManager:
    def __init__(self, 
                 product_file:str, 
                 sqlite_db:SQLiteWrapper):
        self._logger = logging.getLogger(__name__)
        self.sqlite_db = sqlite_db
        self.product_file = product_file
        self.product_data = []
        self.product_map = {}

    def _sub_parse_parts(self, value):
        p = value.split('/')

    def _sub_parse(self, asset, container_name, sub_container_name):
        objects=set()
        if isinstance(asset, dict):
            if container_name in asset:
                parts = asset[container_name]
                for k in parts.keys():
                    if k == sub_container_name:
                        o = parts[k]
                        if isinstance(o, str):
                            o=o.strip()
                            if len(o) >0:
                                objects.add(o)
                        elif isinstance(o, dict):
                            if '@VALUE' in o:
                                v = o['@VALUE']
                                p = v.split('/')
                                [objects.add(x.strip()) for x in p]
                        elif isinstance(o, list):
                            for x in o:
                                v = x['@VALUE']
                                p = v.split('/')
                                [objects.add(x.strip()) for x in p]

        objects.discard("")  
        objects.discard("Documents")
        objects.discard("Support")
        objects.discard("Default")
        return objects
    
    def _extract_product_metadata(self, product) -> tuple[list, list, list]:
        rawxml  = product['raw_metadata']
        tojson  = xmltodict.parse(rawxml)

        assetlist = tojson['ContentDBInstall']['Products']['Product']['Assets']['Asset']
        compatlist = set()
        contenttype = set()
        categories = set()

        #print (f"CT CHECK {assetlist}")

        for asset in assetlist:

            if isinstance(asset, dict):

                if 'ContentType' in asset:
                    test = asset['ContentType']['@VALUE']
                    if test not in ["", "Documents","Support","Default"]:
                        contenttype.add (asset['ContentType']['@VALUE']) 

                if 'Compatibilities' in asset:
                    compatlist |= (self._sub_parse(asset, 'Compatibilities', 'Compatibility'))

                if 'Categories' in asset:
                    categories |= (self._sub_parse(asset, 'Categories', 'Category'))

        return list(compatlist), list(contenttype), list(categories)


    def _update_product(self, product) -> dict:
        sku = product.get("sku").strip()
        if len(sku) > 0:
            self.product_map[sku] = product

        compatabilities, content_types, tags = \
            self._extract_product_metadata(product)
        
        del product["raw_metadata"]
        product["compatabilities"]  = compatabilities
        product["content_types"]    = content_types
        product["tags"]             = tags

        return product

    def _fetch_daz_metadata(self) -> dict:
        script_name = "ListProductsMetadataSA.dsa"
        script_args = [self.product_file]
        rv = run_daz_script(script_name, script_args)    
        if not rv:
            self._logger.error("Error: DAZ script execution failed.")
            return {}
        else:
            products = {}
            product_data = json.load(open(self.product_file, "r"))
            for product in product_data:
                self._update_product(product)
                products[product['sku']]=product

            self._logger.info (f"Info : Load {len(self.product_map)} from local database.  ")


        return products
        
    def _crush_keylist(self, mark_list):
        cats=[]
        for obj in mark_list:
            cats.append(obj['category'])
        return ",".join(cats)
    
    def extract_daz_content(self, all_skus=False):
        # Prefetch the basic product data set
        products    = self._fetch_daz_metadata()

        print (f'Extracted product count is {len(products)}')

        # Then figure out which SKUs are part of the update set
        new_skus    = []
        if all_skus:
            new_skus = list(products.keys())
        else:
            for sku in self.product_map.keys():
                product = products[sku]
                row     = self.sqlite_db.get_sku_row(sku)
                if row is None:
                    new_skus.append(sku)

        print (f'Loaded SKU count is {len(new_skus)} all={all_skus}')

        return

        # For each item in the update set, we need to get the product description   
        x=0
        for sku in new_skus:
            x+=1
            product = products[sku]
            slab_url = f"http://www.daz3d.com/dazApi/slab/{sku}"
            slab_content    = fetch_json_from_url (slab_url)
            
            if slab_content is not None:
                product_url     = f"http://www.daz3d.com/{slab_content['url']}"    
                image_url       = slab_content['imageUrl']
                image_url       = image_url[image_url.index("/http")+1:]
                mature          = slab_content['mature']
                html_content    = fetch_html_content(product_url)
                if html_content is not None:
                    soup = BeautifulSoup(html_content, 'html.parser')
                    
                    title   = str(soup.select_one('meta[property="og:title"]'))
                    title   = title[title.index("content=\"")+len("content=\""):title.index("\" property")]
                    title   = html.unescape(title)

                    try:
                        description = str(soup.select_one('meta[property="og:description"]'))
                        description=description[description.index("content=\"")+len("content=\""):description.index("\" property")]
                        description=html.unescape(description)
                        description=description.replace("&nbsp;", "&")
                    except ValueError as ve:
                        print (f'Failed to parse description for sky {sku}')
                        description="UNKNOWN"

                    product['title']=title
                    product['description']=description
                    product['image_url']=image_url
                    product['mature']=mature

                    # CLean up the insertion set 
                    row={}
                    for key in product.keys():
                        item=product[key]
                        if isinstance(item, list):
                            row[key]=",".join(item)
                        elif isinstance(item, bool):
                            row[key]=str(item)
                        elif isinstance(item, Tag):
                            row[key]=item.text
                        else:
                            row[key]=item


                    # We should be able to insert a row into the Sqlite database at this point, to create the base 
                    # information about a product 


                    sql_row = {
                        'sku': row['sku'],
                        'url': product_url,
                        'image_url': row['image_url'],
                        'name': product['title'],
                        'description': row["description"],
                        'artist': row['raw_artists'],
                        'tags': row['tags'],
                        'compatible_figures': row['compatabilities'],
                        'category': row['content_types'],
                        'mature': mature,
                        'last_updated': row['date_last_updated']
                    }

                    
                    # print (f"####################### INSERT {sku} ##################")
                    # for key in sql_row.keys():
                    #     value = sql_row[key]
                    #     print (f"{type(value)}: {key}={value}")
                    # print ("#########################################################")

                    #print (f"TOSQL = {json.dumps(sql_row, indent=2)}")

                    rv = self.sqlite_db.insert_item(sql_row)

                    if rv:
                        print (
                            f"Info: ({x}/{len(new_skus)}) Added item {sku}: {title}"
                        )
                    else:
                        print (
                            f"Error: ({x}/{len(new_skus)}) Failed to add item {sku}: {title}"
                        )
                    
# if __name__ == '__main__':
#     from managers import daz_db_manager
#     daz_db_manager.process_slab(87743)
#     #daz_db_manager.pre_fetch_faz_data()
#     #daz_db_manager.process_extended_content()
