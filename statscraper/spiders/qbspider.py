import scrapy
import pandas as pd
import json

def get_dataframe_as_dict(response_html):
    #Take raw table html and return as a dictionary
    df = pd.read_html(response_html, header=1)[0]
    df_json_string = df.to_json()
    return json.loads(df_json_string)

class StatSpider(scrapy.Spider):
    name = 'qb'

    YEARS = [2019,2020,2021]

    QB_URL_STUBS = {}

    def start_requests(self):  
        for year in self.YEARS:
            PLAYER_DF = pd.read_json('https://raw.githubusercontent.com/jdellape/pro-football-scrapy/main/players.json')
            QB_DF = PLAYER_DF[PLAYER_DF['position']=='QB']
            QB_DF = QB_DF[QB_DF['year']==year]
            self.QB_URL_STUBS[year] = [url_segment.split('.')[0] + '/' for url_segment in list(QB_DF.url)]
        
        urls_to_scrape = []
        for year in self.YEARS:
            for stub in self.QB_URL_STUBS[year]:
                adv_stats_url =  f'https://www.pro-football-reference.com{stub}gamelog/{str(year)}/advanced'
                reg_stats_url = f'https://www.pro-football-reference.com{stub}gamelog/{str(year)}'
                urls_to_scrape.append(adv_stats_url)
                urls_to_scrape.append(reg_stats_url)
                #urls_to_scrape.append({'player':stub, 'year': year, 'adv_stats_url':adv_stats_url, 'reg_stats_url':reg_stats_url})

        for url in urls_to_scrape:
            #print(url)
            #yield both the advanced and the regular stats url
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        if response.url.split("/")[-1] == 'advanced':
            year = response.url.split("/")[-2]
            player_id = ('/').join(response.url.split("/")[4:6])
            #Isolate advanced passing table
            adv_pass_table_html = response.xpath('//table[@id="advanced_passing"]').extract()[0]
            parsed_adv_pass = get_dataframe_as_dict(adv_pass_table_html)
            
            #Isolate advanced rushing and receiving table
            adv_rush_rec_table_html = response.xpath('//table[@id="advanced_rushing_and_receiving"]').extract()[0]
            parsed_adv_rush_rec = get_dataframe_as_dict(adv_rush_rec_table_html)

            #output to dict format in .json file
            yield {
                    'player_id':player_id,
                    'year': year,
                    'category': 'advanced',
                    'pass_table': parsed_adv_pass,
                    'rush_rec_table': parsed_adv_rush_rec
                }
        else:
            year = response.url.split("/")[-1]
            player_id = ('/').join(response.url.split("/")[4:6])
            #Isolate regular stats
            reg_pass_table_html = response.xpath('//table[@id="stats"]').extract()[0]
            parsed_reg_pass = get_dataframe_as_dict(reg_pass_table_html)
            yield {
                    'player_id': player_id,
                    'year': year,
                    'category': 'regular',
                    'pass_table': parsed_reg_pass
                }
    