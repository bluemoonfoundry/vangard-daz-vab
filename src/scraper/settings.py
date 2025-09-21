# asset_scraper/settings.py

# Scrapy settings for asset_scraper project

BOT_NAME = 'asset_scraper'

# CHANGE THESE TWO LINES
SPIDER_MODULES = ['scraper.spiders']
NEWSPIDER_MODULE = 'scraper.spiders'


# --- Politeness and Crawl Configuration ---
# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Set a custom User-Agent to identify your bot
USER_AGENT = 'VisualAssetBrowserBot/1.0 (For personal content indexing; https://your-project-url.com)'

# Configure a delay for requests for the same website (e.g., 2 seconds)
DOWNLOAD_DELAY = 2
CONCURRENT_REQUESTS_PER_DOMAIN = 8


# --- Playwright Settings for JavaScript Rendering ---
# Enable the Playwright download handler for both http and https schemes.
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

# Set the required Twisted reactor for asyncio compatibility.
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"


# --- Item Pipeline Configuration ---
# The order is important: process the item first, then save it to the database.
# Also update the ITEM_PIPELINES paths
ITEM_PIPELINES = {
   'scraper.pipelines.AssetProcessingPipeline': 300, # <-- CHANGED
   'scraper.pipelines.SQLitePipeline': 400,          # <-- CHANGED
}


# --- Custom Settings for Pipelines ---
SQLITE_DB = 'products.db'
SQLITE_TABLE = 'product'


# --- Standard Scrapy Settings ---
REQUEST_FINGERPRINTER_IMPLEMENTATION = '2.7'
FEED_EXPORT_ENCODING = 'utf-8'