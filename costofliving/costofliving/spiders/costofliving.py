from .cities import cities
from datetime import date

import scrapy
import logging
from scrapy.utils.log import configure_logging


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
        cost_of_living_metrics = {}
        city = response.url.split("/")[-1].split("?")[0]

        cost_of_living_metrics["today"] = date.today().strftime("%Y-%m-%d")
        cost_of_living_metrics["city"] = city
        cost_of_living_metrics["province"] = cities[city]["province"]
        cost_of_living_metrics["country"] = cities[city]["country"]
        cost_of_living_metrics["continent"] = cities[city]["continent"]

        table = response.xpath('.//table[@class="data_wide_table new_bar_table"]')

        for tr in table.xpath(".//tr"):
            if not tr.xpath(".//th"):
                col_cat = tr.xpath(".//td/text()").get().strip()
                # col_cat = col_cat.lower().replace(" (%)", "").replace(",", "_").replace(" ...", "").replace(".", "_")
                # col_cat = col_cat.replace("(", "").replace(")", "").replace("/", "").replace("h&m", "hnm")
                # col_cat = col_cat.replace(" ", "_").replace("-", "_")
                prices = tr.xpath(".//td/span/text()").getall()
                price = prices[0].strip().replace("\xa0$", "").replace(",", "")
                price_range = ' - '.join(prices[1:]).strip()
                cost_of_living_metrics[col_cat] = price
                cost_of_living_metrics[f'{col_cat}_pr'] = price_range

        cost_of_living_metrics["last_update"] = response.xpath('//div[@class="align_like_price_table"]/text()').getall()[-1].split(": ")[1].strip()

        yield cost_of_living_metrics
