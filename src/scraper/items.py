# asset_scraper/items.py

import scrapy

class AssetItem(scrapy.Item):
    # Core Scraped Data
    url = scrapy.Field()
    image_url = scrapy.Field()
    sku = scrapy.Field()
    store = scrapy.Field()
    name = scrapy.Field()
    artist = scrapy.Field()
    price = scrapy.Field()
    description = scrapy.Field()
    tags = scrapy.Field()
    
    # Technical Metadata
    formats = scrapy.Field()
    poly_count = scrapy.Field()
    textures_info = scrapy.Field()
    
    # Compatibility Metadata
    required_products = scrapy.Field()
    compatible_figures = scrapy.Field()
    compatible_software = scrapy.Field()
    
    # Generated Fields (created in pipelines)
    embedding_text = scrapy.Field()
    last_updated = scrapy.Field()

    # Additional fields 
    category = scrapy.Field()
    mature = scrapy.Field()