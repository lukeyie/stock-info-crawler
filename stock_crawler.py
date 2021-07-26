from io import StringIO
from datetime import datetime
import math
from decimal import Decimal, ROUND_HALF_UP
from urllib.parse import urlencode
import json

import requests
import pandas as pd

from dto import StockInfoDTO, StockDTO, IncomeStatementDTO, \
    DateInfoDTO, FinMindFinancialStatementsDTO


class StockCrawler:
    __DATE_FORMAT = '%Y-%m-%d'

    __API_CONFIG_PATH = '.\\api.config'

    __TWSE_LISTED_STOCKS_URL = 'http://isin.twse.com.tw/isin/C_public.jsp' \
        '?strMode=2'
    __OTC_LISTED_STOCKS_URL = 'https://isin.twse.com.tw/isin/C_public.jsp' \
        '?strMode=4'
    __YAHOO_FINANCE_API_URL = 'https://query1.finance.yahoo.com/v7/finance/' \
        'download/'
    __FINMIND_API_URL = 'https://api.finmindtrade.com/api/v4/data'

    def __init__(self):
        input_file = open(self.__API_CONFIG_PATH)
        self.__api_config = json.load(input_file)
        self.stocks_list = self.__get_tw_available_stocks_list()

    def get_price_and_vol(self, ticker, start_date, end_date):
        start_date_timestamp = self.__create_timestamp(start_date)
        end_date_timestamp = self.__create_timestamp(end_date)

        is_twse = self.stocks_list[ticker].market == 'TWSE'
        target_url = self.__build_crawl_target_url(
            ticker, start_date_timestamp, end_date_timestamp, is_twse=is_twse)

        try:
            response = requests.get(
                target_url,
                headers={
                    'Connection': 'close',
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'
                    'AppleWebKit/537.36 (KHTML, like Gecko) Chrome/'
                    '91.0.4472.164 Safari/537.36'
                }
            )

            stock_info_df = pd.read_csv(
                StringIO(response.text), error_bad_lines=False)
            stock_dto = StockDTO(
                ticker=ticker, stock_name=self.stocks_list[ticker].stock_name)
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
                date_info_dto = DateInfoDTO(
                    date=stock_info_df["Date"].iloc[i],
                    open=open,
                    close=close,
                    high=high,
                    low=low,
                    volume=volume
                )
                stock_dto.date_info.append(date_info_dto)

            return stock_dto
        except Exception as e:
            if response.json()['error'] \
                    and response.json()['error']['code'] == 'Unauthorized':
                raise ValueError('Yahoo finance disconnected')

            print(f'Error: {e}')
            print(f'## Warning: Ticker {ticker} is failed!')

    def get_income_statements_dto(self, ticker, start_year, start_season,
                                  end_year, end_season):

        [start_year, start_season, end_year, end_season] = \
            [int(start_year), int(start_season),
             int(end_year), int(end_season)]

        try:
            response = requests.get(
                self.__build_income_statement_url(
                    ticker, start_year, start_season, end_year, end_season),
                headers={'Connection': 'close'}
            ).json()

            if response['status'] == 402:
                raise Exception('Reach the FindMind limit')
        except Exception as e:
            raise Exception(f'Ticker: {ticker}, Error: {e}')

        data_dict = {}
        for item in response['data']:
            find_mind_dto = FinMindFinancialStatementsDTO.parse_obj(item)

            date = datetime.strptime(find_mind_dto.date, self.__DATE_FORMAT)
            if(date not in data_dict):
                income_statement_dto = IncomeStatementDTO(
                    year=date.year, season=self.__get_season(date.month))
                data_dict.update({date: income_statement_dto})

            self.__build_income_statement_dto_from_finmind_api(
                data_dict[date], find_mind_dto)

        stock_dto = StockDTO(
            ticker=ticker, stock_name=self.stocks_list[ticker].stock_name)
        for key in data_dict:
            stock_dto.income_statements.append(data_dict[key])

        return stock_dto

    def __build_crawl_target_url(self, ticker, start_date_timestamp,
                                 end_date_timestamp, is_twse=True):
        query_str = urlencode({
            'period1': str(start_date_timestamp),
            'period2': str(end_date_timestamp),
            'interval': '1d',
            'events': 'history',
            'crumb': 'hP2rOschxO0'
        })

        twse_target_url = \
            self.__YAHOO_FINANCE_API_URL + f'{ticker}.TW?' + query_str
        otc_target_url = \
            self.__YAHOO_FINANCE_API_URL + f'{ticker}.TWO?' + query_str

        return twse_target_url if is_twse else otc_target_url

    def __build_income_statement_url(self, ticker, start_year, start_season,
                                     end_year, end_season):
        query_str = urlencode({
            'dataset': 'TaiwanStockFinancialStatements',
            'data_id': ticker,
            'start_date': self.__get_date_from_season(
                start_year, start_season),
            'end_date': self.__get_date_from_season(
                end_year, end_season),
            'token': self.__api_config['finmind_api_token']
        })

        return self.__FINMIND_API_URL + '?' + query_str

    def __create_timestamp(self, date):
        date_time = datetime.strptime(date, self.__DATE_FORMAT)
        timestamp = datetime.timestamp(date_time)
        return int(timestamp)

    def __get_season(self, month):
        if month >= 1 and month <= 3:
            return 1
        elif month >= 4 and month <= 6:
            return 2
        elif month >= 7 and month <= 9:
            return 3
        else:
            return 4

    def __get_date_from_season(self, year, season):
        if season == 1:
            return f'{year}-01-01'
        elif season == 2:
            return f'{year}-04-01'
        elif season == 3:
            return f'{year}-07-01'
        elif season == 4:
            return f'{year}-10-01'

    def __get_tw_available_stocks_list(self):
        twse_stocks_list = self.__get_stocks_list(
            self.__TWSE_LISTED_STOCKS_URL)
        otc_stocks_list = self.__get_stocks_list(self.__OTC_LISTED_STOCKS_URL)

        twse_stocks_list.update(otc_stocks_list)
        return dict(sorted(twse_stocks_list.items(), key=lambda item: item[0]))

    def __get_stocks_list(self, url):
        market = 'TWSE' if url == self.__TWSE_LISTED_STOCKS_URL else 'OTC'
        unformatted_stocks_table = requests.get(url,
                                                headers={'Connection': 'close'})
        unformatted_stocks_df = pd.read_html(unformatted_stocks_table.text)[0]
        unformatted_stocks_df.columns = unformatted_stocks_df.iloc[0]

        # Remove none stock rows
        stocks = {}
        for i in range(0, len(unformatted_stocks_df.index)):
            if unformatted_stocks_df['CFICode'].iloc[i] != 'ESVUFR':
                continue
            ticker_and_name = (unformatted_stocks_df['有價證券代號及名稱']
                               .iloc[i].replace(u'\u3000', ' ').split(' '))
            stock_info_dto = StockInfoDTO()
            stock_info_dto.stock_name = ticker_and_name[1]
            stock_info_dto.sector = unformatted_stocks_df["產業別"].iloc[i]
            stock_info_dto.market = market
            stocks.update({f'{ticker_and_name[0]}': stock_info_dto})

        return stocks

    def __build_income_statement_dto_from_finmind_api(
            self, income_statement_dto,
            finmind_api_dto: FinMindFinancialStatementsDTO):

        btax = ['IncomeBeforeTaxFromContinuingOperations',
                'IncomeBeforeIncomeTax', 'PreTaxIncome']
        ni = ['NetIncome', 'IncomeAfterTaxes']

        if finmind_api_dto.type == 'Revenue':
            income_statement_dto.revenue = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'CostOfGoodsSold':
            income_statement_dto.cost = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'GrossProfit':
            income_statement_dto.gp = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'OperatingExpenses':
            income_statement_dto.oe = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'OperatingIncome':
            income_statement_dto.oi = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'TotalNonoperatingIncomeAndExpense':
            income_statement_dto.nie = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'TotalNonbusinessIncome':
            income_statement_dto.nie += int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'TotalnonbusinessExpenditure':
            income_statement_dto.nie -= int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type in btax:
            income_statement_dto.btax = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type in ni:
            income_statement_dto.ni = int(finmind_api_dto.value / 1000)
        elif finmind_api_dto.type == 'EPS':
            income_statement_dto.eps = finmind_api_dto.value
