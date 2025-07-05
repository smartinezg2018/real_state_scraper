import scrapy
import json

class RealstateSpider(scrapy.Spider):
    name = 'realstate'
    allowed_domains = ['fincaraiz.com.co']
    max_page = -1
    url = "https://www.fincaraiz.com.co/venta/casas-y-apartamentos/medellin/antioquia"

    async def start(self):
        # Send a request to get max page number
        first_page_url = ''.join([self.url, "/pagina1?&ordenListado=3"])
        yield scrapy.Request(url=first_page_url, callback=self.first_page)
    
    def parse(self, response):
        properties = response.xpath("//div[contains(@class, 'listingCard')]")
        
        for property in properties:
            link = property.xpath(".//a[@class='lc-cardCover']/@href").get()
            title = property.xpath(".//a[@class='lc-cardCover']/@title")
            price = property.xpath(".//div[@class='lc-dataWrapper']/a/div[@class='lc-price']//strong/text()").get()

            full_url = ''.join(['https://fincaraiz.com.co', link])

            # Passing simple parse values to detailed parse.
            yield scrapy.Request(
                url=full_url,
                callback=self.parse_detail,
                meta={
                    'url': full_url,
                    'title': title,
                    'price': price
                }            
            )

    def parse_detail(self, response):
        # Basic parse data

        url = response.meta['url']
        title = response.meta.get('title')
        price = response.meta.get('price')

        # Div indexing according to @class = 'technical_sheet' div
        technical_sheet_index = {
            "property_type": 2,
            "stratum": 1,
            "bathrooms": 4,
            "sqmeters_built": 5,
            "sqmeters_private": 6,
            "property_age": 7,
            "bedrooms": 8,
            "parking_spots": 9,
            "administration_fees": 10,
            "internal_floor_number": 12
        }
        
        neighborhood = title.re_first(r'Venta en\s+([^\s,]+(?:\s+[^\s,]+)*)\s*,') 
        city = title.re_first(r',\s*(.+)$')
        

        # Combination of new and extracted information
        result = {
           'url': url,
           'city': city,
           'neighborhood': neighborhood,
           'price': price,
       } | {
           k: self.detail_getter(response, v)
           for k, v in technical_sheet_index.items()
       }
        
        with open('scrapped.jsonl', 'a', encoding='utf-8') as f:
           json.dump(result, f, ensure_ascii=False)
           f.write('\n')
                
        
    def detail_getter(self, response, index):
        return response.xpath(f"//div[contains(@class,'technical-sheet')]//div[contains(@class, 'ant-row')][{index}]//strong/text()").get()
    

    def first_page(self, response):
        # Process first page
        self.parse(response)


        # Process remaining pages...
        # Get the total number of pages and change attribute
        max_page_text = response.xpath('//li[contains(@class, "ant-pagination-item")][last()-1]/a/text()').get()

        try:
            max_page = int(max_page_text)
        except:
            max_page = 1
                
        # Process page 2 onwards
        for n in range(2, max_page + 1):
            page_url = ''.join([self.url, f"/pagina{n}?&ordenListado=3"])
            yield scrapy.Request(url=page_url, callback=self.parse)