import scrapy

class StatSpider(scrapy.Spider):
    name = 'stats'

    #Make a request to the fantasy football landing page for seasons 2019 - 2021
    def start_requests(self): 
        years = ['2019','2020','2021']
        start_urls = [f'https://www.pro-football-reference.com/years/{year}/fantasy.htm' for year in years]

        for url in start_urls:
            yield scrapy.Request(url=url, callback=self.parse)

    #For each scrapy request, parse out player names, positions, and unique player base urls
    def parse(self, response):
        year = response.url.split("/")[-2]
        player_names = response.xpath('//td[@data-stat="player"]/a/text()').getall()
        player_urls = response.xpath('//td[@data-stat="player"]/a[contains(@href, "players")]/@href').getall()
        player_pos = response.xpath('//td[@data-stat="fantasy_pos"]/text()').getall()

        for name, url, position in zip(player_names, player_urls, player_pos):
            yield {
                'year': year,
                'position': position,
                'name': name,
                'url': url
            }