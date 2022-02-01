import scrapy

class StatSpider(scrapy.Spider):
    name = 'stats'

    def start_requests(self): 
        years = ['2019','2020','2021']
        start_urls = [f'https://www.pro-football-reference.com/years/{year}/fantasy.htm' for year in years]

        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        year = response.url.split("/")[-2]
        player_names = response.xpath('//td[@data-stat="player"]/a/text()').getall()
        player_urls = response.xpath('//a[contains(@href, "players")]/@href').getall()

        for name, url in zip(player_names, player_urls):
            yield {
                'year': year,
                'name': name,
                'url':url
            }