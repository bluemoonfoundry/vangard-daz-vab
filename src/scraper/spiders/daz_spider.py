from .base_spider import BaseAssetSpider


class DazSpider(BaseAssetSpider):
    name = "daz"
    allowed_domains = ["daz3d.com"]
    store_name = "DAZ 3D"

    selectors = {
        "name": "h1.product-name::text",
        "sku": '//div[contains(text(), "SKU")]/following-sibling::div[@class="data"]/text()',
        "artist": "//div[contains(text(), 'Artist')]/following-sibling::div[@class='data']//a/text()",
        "price": "div.price-final span.price::text",
        "description": "//h3[contains(text(), 'Details')]/following-sibling::div[@class='std']//text()",
        "tags": "div.product-tags a::text",
        "required_products": "//div[contains(text(), 'Required Products')]/following-sibling::div[@class='data']//a/text()",
        "compatible_figures": "//div[contains(text(), 'Compatible Figures')]/following-sibling::div[@class='data']//a/text()",
        "compatible_software": "//strong[contains(text(), 'Compatible Software')]/parent::div/following-sibling::text()",
        "formats": '//dt[contains(text(), "File Formats")]/following-sibling::dd/text()',
    }
