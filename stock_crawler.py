from io import StringIO
import datetime
import math
from decimal import Decimal, ROUND_HALF_UP

import requests
import pandas as pd


class StockCrawler:
    __DATE_FORMAT = '%Y-%m-%d'
    __TWSE_LISTED_STOCKS_URL = 'http://isin.twse.com.tw/isin/C_public.jsp' \
        '?strMode=2'
    __OTC_LISTED_STOCKS_URL = 'https://isin.twse.com.tw/isin/C_public.jsp' \
        '?strMode=4'

    def __init__(self):
        self.stocks_list = self.__get_tw_available_stocks_list()

    def get_price_and_vol(self, start_date, end_date, ticker):
        start_date_timestamp = self.__create_timestamp(start_date)
        end_date_timestamp = self.__create_timestamp(end_date)

        target_url = self.__build_crawl_target_url(
            ticker, start_date_timestamp, end_date_timestamp)
        try:
            response = requests.get(target_url)
            stock_info_df = pd.read_csv(StringIO(response.text))
            stock_info = {
                'ticker': f'{ticker}',
                'stock_name': f'{self.stocks_list[ticker]["stock_name"]}',
                'date_info': []
            }
            for i in range(len(stock_info_df.index)):
                open = Decimal(stock_info_df['Open'].iloc[i]) \
                    .quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                close = Decimal(stock_info_df['Close'].iloc[i]) \
                    .quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                high = Decimal(stock_info_df['High'].iloc[i]) \
                    .quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                low = Decimal(stock_info_df['Low'].iloc[i]) \
                    .quantize(Decimal('.01'), rounding=ROUND_HALF_UP)
                volume = \
                    int(round(stock_info_df["Volume"].iloc[i], -3) / 1000) \
                    if not math.isnan(stock_info_df["Volume"].iloc[i]) else 0
                stock_info['date_info'].append(
                    {
                        'date': f'{stock_info_df["Date"].iloc[i]}',
                        'open': f'{open}',
                        'close': f'{close}',
                        'high': f'{high}',
                        'low': f'{low}',
                        'volume': f'{volume}'
                    }
                )
            return stock_info
        except Exception:
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

    def __get_tw_available_stocks_list(self):
        twse_stocks_list = \
            self.__get_stocks_list(self.__TWSE_LISTED_STOCKS_URL)
        otc_stocks_list = \
            self.__get_stocks_list(self.__OTC_LISTED_STOCKS_URL)

        twse_stocks_list.update(otc_stocks_list)
        return twse_stocks_list

    def __get_stocks_list(self, url):
        unformatted_stocks_table = requests.get(url)
        unformatted_stocks_df = pd.read_html(unformatted_stocks_table.text)[0]
        unformatted_stocks_df.columns = unformatted_stocks_df.iloc[0]

        # Remove none stock rows
        stocks = {}
        for i in range(0, len(unformatted_stocks_df.index)):
            if unformatted_stocks_df['CFICode'].iloc[i] != 'ESVUFR':
                continue
            ticker_and_name = (unformatted_stocks_df['有價證券代號及名稱']
                               .iloc[i].replace(u'\u3000', ' ').split(' '))
            stocks.update(
                {
                    f'{ticker_and_name[0]}': {
                        'stock_name': f'{ticker_and_name[1]}',
                        'sector':  f'{unformatted_stocks_df["產業別"].iloc[i]}'
                    }
                }
            )

        return stocks
