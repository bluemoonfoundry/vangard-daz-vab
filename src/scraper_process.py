import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.settings import Settings
from scraper.spiders.daz_spider import DazSpider
from scraper import settings as scrapy_settings_module

def run_scraper(products_to_scrape: list):
    """
    Runs the Scrapy CrawlerProcess for a given list of product data.
    """
    if not products_to_scrape:
        print("No products to scrape.")
        return

    print(f"--- Starting Scrapy Process for {len(products_to_scrape)} products ---")
    
    settings = Settings()
    settings.setmodule(scrapy_settings_module)
    
    process = CrawlerProcess(settings)
    
    # --- THIS IS THE KEY CHANGE ---
    # Pass the list of products directly to the spider's constructor
    process.crawl(DazSpider, products=products_to_scrape)
    
    process.start()
    print("--- Scrapy Process Finished ---")