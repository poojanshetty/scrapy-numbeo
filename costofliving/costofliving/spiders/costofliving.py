from .cities import cities
from datetime import date

import scrapy
import logging
from scrapy.utils.log import configure_logging

from ..items import CostoflivingItem


configure_logging(install_root_handler=False)
logging.basicConfig(
    filename="cost_of_living.txt",
    format="%(levelname)s: %(message)s",
    level=logging.ERROR
)


class CostOfLivingSpider(scrapy.Spider):
    name = "costofliving"
    start_urls = [f"https://www.numbeo.com/cost-of-living/in/{city}?displayCurrency=USD" for city, _ in cities.items()]

    def parse(self, response):
        item = CostoflivingItem()
        url_city = response.url.split("/")[-1].split("?")[0]

        item["today"] = date.today().strftime("%Y-%m-%d")
        item["city"] = cities[url_city]["city"]
        item["province"] = cities[url_city]["province"]
        item["country"] = cities[url_city]["country"]
        item["continent"] = cities[url_city]["continent"]

        table = response.xpath('.//table[@class="data_wide_table new_bar_table"]')

        for tr in table.xpath(".//tr"):
            if not tr.xpath(".//th"):
                col_cat = tr.xpath(".//td/text()").get().strip()

                # clean column names, so it can be stored in the database easier
                col_cat = col_cat.lower().replace(" (%)", "").replace(" h&m,", "hnm").replace(",", "_")
                col_cat = col_cat.replace(" ...", "").replace(".", "_").replace("(", "").replace(")", "")
                col_cat = col_cat.replace("/", "").replace(" ", "_").replace("-", "_").replace("+", "_plus")
                col_cat = col_cat.replace("1_pair", "one_pair").replace("1_summer", "one_summer")
                col_cat = col_cat.replace("buffalo", "beef")  # Buffalo in India, Beef elsewhere

                prices = tr.xpath(".//td/span/text()").getall()
                price = prices[0].strip().replace("\xa0$", "").replace(",", "")
                price_range = ' - '.join(prices[1:]).strip()
                item[col_cat] = price
                item[f'{col_cat}_pr'] = price_range

        last_update = response.xpath('//div[@class="align_like_price_table"]/text()').getall()[-1].split(": ")[1]
        item["last_update"] = last_update.strip()

        yield item
