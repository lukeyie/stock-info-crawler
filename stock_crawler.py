from io import StringIO
import datetime
from pandas.core.frame import DataFrame

import requests
import pandas as pd


class StockCrawler:
    __DATE_FORMAT = '%Y-%m-%d'
    __TW_AVAILABLE_STOCKS_URL = 'http://isin.twse.com.tw/isin/C_public.jsp' \
                                '?strMode=2'
    
    def __init__(self, start_date, end_date, ticker=None):
        if ticker:
            self.ticker = str(ticker)
            
        self.start_date = start_date;
        self.end_date = end_date;
        self.__start_date_timestamp = self.__create_timestamp(self.start_date)
        self.__end_date_timestamp = self.__create_timestamp(self.end_date)
        
        self.__get_tw_available_stocks_list()
        
    def get_price_and_vol(self, ticker=None):
        if ticker:
            self.ticker = str(ticker)
        if not self.ticker:
            raise ValueError('Need a ticker to start crawling,' \
                              'please add to the argument')
        
        target_url = self.__build_crawl_target_url(
            self.ticker, self.__start_date_timestamp, 
            self.__end_date_timestamp)

        try:
            response = requests.get(target_url)
            stock_info_df = pd.read_csv(StringIO(response.text))
            stock_info = {
                'ticker': f'{self.ticker}', 
                'stockName': f'{self.stocks_list[self.ticker]["stockName"]}',
                'dateInfo': []
            }
            for i in range(len(stock_info_df.index)):
                stock_info['dateInfo'].append(
                    {
                        'date': f'{stock_info_df["Date"].iloc[i]}',
                        'open': f'{stock_info_df["Open"].iloc[i]}',
                        'close': f'{stock_info_df["Close"].iloc[i]}',
                        'high': f'{stock_info_df["High"].iloc[i]}',
                        'low': f'{stock_info_df["Low"].iloc[i]}'
                    }
                )
            
            return stock_info
        except Exception as e:
            raise e
            print(f'## Warning: Ticker {ticker} is failed!')
    
    def __build_crawl_target_url(self, ticker, start_date_timestamp, 
                                end_date_timestamp):
        crawl_target_url = 'https://query1.finance.yahoo.com/' \
                           f'v7/finance/download/{ticker}.TW' \
                           f'?period1={str(start_date_timestamp)}' \
                           f'&period2={str(end_date_timestamp)}' \
                           '&interval=1d&events=history&crumb=hP2rOschxO0'
                             
        return crawl_target_url
    
    def __create_timestamp(self, date):
        date_time = datetime.datetime.strptime(date, self.__DATE_FORMAT)
        timestamp = datetime.datetime.timestamp(date_time)
        return int(timestamp)
    
    def __get_tw_available_stocks_list(self) :
        unformatted_stocks_table = requests.get(self.__TW_AVAILABLE_STOCKS_URL)
        unformatted_stocks_df = pd.read_html(unformatted_stocks_table.text)[0]
        unformatted_stocks_df.columns = unformatted_stocks_df.iloc[0]

        # Remove none stock rows
        stocks = {}
        for i in range(2, len(unformatted_stocks_df.index)) :
            if unformatted_stocks_df.iloc[i][0] == '上市認購(售)權證':
                break
            ticker_and_name = (unformatted_stocks_df['有價證券代號及名稱']
                               .iloc[i].replace(u'\u3000', ' ').split(' '))
            stocks.update(
                {
                    f'{ticker_and_name[0]}': {
                        'stockName': f'{ticker_and_name[1]}',
                        'sector':  f'{unformatted_stocks_df["產業別"].iloc[i]}'
                    }
                }
            )
        self.stocks_list = stocks
        
s = StockCrawler("2021-05-01", "2021-05-20", ticker=2330)
print(s.get_price_and_vol())