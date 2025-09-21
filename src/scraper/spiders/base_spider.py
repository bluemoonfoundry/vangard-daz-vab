import scrapy

from scraper.items import AssetItem


class BaseAssetSpider(scrapy.Spider):
    store_name = "Unknown"
    # Selectors are defined in subclasses

    def __init__(self, products=None, *args, **kwargs):
        super(BaseAssetSpider, self).__init__(*args, **kwargs)
        # The list of {'url': '...', 'sku': '...'} is now an attribute of the spider
        self.products_to_scrape = products
        # We no longer use the 'start_urls' attribute from the CrawlerProcess
        self.start_urls = []

    async def start(self):
        for req in self.start_requests():
            yield req

    def start_requests(self):
        """
        This method now iterates over our custom product list, yielding
        a request for each one and passing the SKU in the request's meta.
        """
        if not self.products_to_scrape:
            self.logger.warning("No products passed to spider. Nothing to scrape.")
            return

        for product in self.products_to_scrape:
            yield scrapy.Request(
                url=product["url"],
                callback=self.parse_product,
                meta={
                    "playwright": True,
                    "sku": product["sku"],
                    "image_url": product.get("image_url"),
                    "categoriesData": product.get("categoriesData", []),
                    "figureData": product.get("figureData", []),
                    "mature": product.get("mature", False),
                },
            )

    def parse_product(self, response):
        response.selector.remove_namespaces()
        item = AssetItem()

        # --- THIS IS THE KEY CHANGE ---
        # 1. Get the SKU from the meta dictionary we passed in start_requests.
        item["sku"] = response.meta.get("sku")
        item["mature"] = response.meta.get("mature", False)
        item["image_url"] = response.meta.get("image_url")  # <-- Set it directly

        # Process categoriesData to get the primary category
        categories_data = response.meta.get("categoriesData", [])
        if categories_data and isinstance(categories_data, list):
            # Take the category from the first item in the list
            item["category"] = categories_data[0].get("category")

        # Process figureData to get compatible figures
        figure_data_from_meta = response.meta.get("figureData", [])
        pre_existing_figures = []
        if figure_data_from_meta and isinstance(figure_data_from_meta, list):
            pre_existing_figures = [
                fig.get("category")
                for fig in figure_data_from_meta
                if fig.get("category")
            ]
        # --- END CHANGE ---

        item["url"] = response.url
        item["store"] = self.store_name

        for field in item.fields:
            if field not in ["url", "store", "sku", "mature", "category"] and hasattr(
                self, f"extract_{field}"
            ):
                item[field] = getattr(self, f"extract_{field}")(response)

        # Get the list of figures that was just scraped (it might be an empty list)
        scraped_figures = item.get("compatible_figures", [])

        # Combine the list from the JSON file with the list from the scrape.
        # Using dict.fromkeys() is a fast way to remove duplicates while preserving order.
        combined_figures = list(dict.fromkeys(pre_existing_figures + scraped_figures))

        # Assign the final, complete list back to the item.
        item["compatible_figures"] = combined_figures

        # As a safety check, if scraping for SKU still works, we can log a warning.
        scraped_sku = self.extract_sku(response)
        if scraped_sku and scraped_sku != item["sku"]:
            self.logger.warning(
                f"SKU mismatch for URL {response.url}. Passed in: {item['sku']}, Scraped: {scraped_sku}"
            )

        yield item

    def _execute_selector(self, response, selector, get_all=False):
        if not selector:
            return [] if get_all else None
        clean_selector = selector.strip()
        is_xpath = clean_selector.startswith("/") or clean_selector.startswith("./")
        query = response.xpath(selector) if is_xpath else response.css(selector)
        return query.getall() if get_all else query.get()

    def extract_name(self, response):
        return self._execute_selector(response, self.selectors["name"])

    def extract_sku(self, response):
        return self._execute_selector(response, self.selectors["sku"])

    def extract_artist(self, response):
        return self._execute_selector(response, self.selectors["artist"], get_all=True)

    def extract_price(self, response):
        return self._execute_selector(response, self.selectors["price"])

    def extract_tags(self, response):
        return self._execute_selector(response, self.selectors["tags"], get_all=True)

    def extract_formats(self, response):
        return self._execute_selector(response, self.selectors["formats"], get_all=True)

    #    def extract_poly_count(self, response): return self._execute_selector(response, self.selectors['poly_count'])
    #    def extract_textures_info(self, response): return self._execute_selector(response, self.selectors['textures_info'])
    def extract_required_products(self, response):
        return self._execute_selector(
            response, self.selectors["required_products"], get_all=True
        )

    def extract_compatible_figures(self, response):
        return self._execute_selector(
            response, self.selectors["compatible_figures"], get_all=True
        )

    def extract_compatible_software(self, response):
        return self._execute_selector(
            response, self.selectors["compatible_software"], get_all=True
        )

    def extract_description(self, response):
        parts = self._execute_selector(
            response, self.selectors["description"], get_all=True
        )
        return (
            " ".join(part.strip() for part in parts if part.strip()) if parts else None
        )
