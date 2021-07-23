from io import StringIO
from datetime import datetime
import math
from decimal import Decimal, ROUND_HALF_UP
import time

import requests
import pandas as pd

from dto import StockInfoDTO, StockDTO, IncomeStatementDTO, DateInfoDTO


class StockCrawler:
    __DATE_FORMAT = '%Y-%m-%d'

    __TWSE_LISTED_STOCKS_URL = 'http://isin.twse.com.tw/isin/C_public.jsp' \
        '?strMode=2'
    __OTC_LISTED_STOCKS_URL = 'https://isin.twse.com.tw/isin/C_public.jsp' \
        '?strMode=4'

    __INCOME_STATEMENT_BEFORE_IFRSS_URL = 'https://mops.twse.com.tw/mops/web/' \
        'ajax_t05st32'
    __INCOME_STATEMENT_AFTER_IFRSS_URL = 'https://mops.twse.com.tw/mops/web/' \
        'ajax_t164sb04'

    def __init__(self):
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
            print(response)
            print(response.history)

            stock_info_df = pd.read_csv(StringIO(response.text),
                                        error_bad_lines=False)
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
            print(f'Error: {e}')
            print(f'## Warning: Ticker {ticker} is failed!')

    def get_season_income_statement_dto(self, ticker, year, season):
        roc_year = year - 1911 if year > 1000 else year
        form_data = {
            'encodeURIComponent': 1,
            'step': 1,
            'firstin': 1,
            'off': 1,
            'co_id': ticker,
            'year': roc_year,
            'season': season
        }

        print(f'Start getting {ticker} {year}/S{season} income statement')
        try:
            response = requests.post(
                self.__build_income_statement_url_from_roc_year(year),
                form_data,
                headers={'Connection': 'close'})

            income_statement_df = \
                pd.read_html(response.text, match=('每股盈餘'))[0] \
                .fillna("")
        except Exception as e:
            print(f'Error: {e}')
            print(f'{ticker} getting income statement failed')
        income_statement_df = \
            income_statement_df[income_statement_df.iloc[:, 1] != ""] \
            .iloc[:, 0:2]
        income_statement_df.columns = ['item', 'amount']

        income_statement_dto = IncomeStatementDTO()
        income_statement_dto.year = year
        income_statement_dto.season = season
        for index, row in income_statement_df.iterrows():
            self.__update_value_from_income_statement_df_row(
                income_statement_dto, row)

        return income_statement_dto

    def get_stock_dto_with_income_statement(
            self, ticker, start_date, end_date):
        start_datetime = datetime.strptime(start_date, self.__DATE_FORMAT)
        end_datetime = datetime.strptime(end_date, self.__DATE_FORMAT)
        seasons_diff = self.__get_seasons_diff(start_datetime, end_datetime)

        cur_year = start_datetime.year
        start_season = self.__get_season(start_datetime.month)
        stock_dto = StockDTO(
            ticker=ticker, stock_name=self.stocks_list[ticker].stock_name)
        for i in range(0, seasons_diff + 1):
            cur_season = 4 \
                if (start_season + i) == 4 else (start_season + i) % 4

            income_statement_dto = self.get_season_income_statement_dto(
                ticker, cur_year, cur_season)
            stock_dto.income_statements.append(income_statement_dto)
            time.sleep(50/1000)

            cur_year = cur_year + 1 \
                if cur_season == 4 else cur_year

        return stock_dto

    def __build_crawl_target_url(self, ticker, start_date_timestamp,
                                 end_date_timestamp, is_twse=True):
        twse_target_url = 'https://query1.finance.yahoo.com/' \
            f'v7/finance/download/{ticker}.TW' \
            f'?period1={str(start_date_timestamp)}' \
            f'&period2={str(end_date_timestamp)}' \
            '&interval=1d&events=history&crumb=hP2rOschxO0'

        otc_target_url = 'https://query1.finance.yahoo.com/' \
            f'v7/finance/download/{ticker}.TWO' \
            f'?period1={str(start_date_timestamp)}' \
            f'&period2={str(end_date_timestamp)}' \
            '&interval=1d&events=history&crumb=hP2rOschxO0'

        return twse_target_url if is_twse else otc_target_url

    def __build_income_statement_url_from_roc_year(self, year):
        if year >= 2013:
            return self.__INCOME_STATEMENT_AFTER_IFRSS_URL
        else:
            return self.__INCOME_STATEMENT_BEFORE_IFRSS_URL

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

    def __get_seasons_diff(self, start_datetime, end_datetime):
        start_year = start_datetime.year
        start_season = self.__get_season(start_datetime.month)
        end_year = end_datetime.year
        end_season = self.__get_season(end_datetime.month)

        return (end_year - start_year - 1) * 4 \
            + end_season + (4 - start_season)

    def __get_tw_available_stocks_list(self):
        twse_stocks_list = \
            self.__get_stocks_list(self.__TWSE_LISTED_STOCKS_URL)
        otc_stocks_list = \
            self.__get_stocks_list(self.__OTC_LISTED_STOCKS_URL)

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

    def __update_value_from_income_statement_df_row(
            self, income_statement_dto, row):
        if '營業收入合計' == row[0]:
            income_statement_dto.nor = int(float(row[1]))
        elif '營業成本合計' == row[0]:
            income_statement_dto.cost = int(float(row[1]))
        elif '營業毛利（毛損）' == row[0] or \
                '營業毛利(毛損)' == row[0]:
            income_statement_dto.pro = int(float(row[1]))
        elif '營業費用合計' == row[0]:
            income_statement_dto.oe = int(float(row[1]))
        elif '營業利益（損失）' == row[0] or \
                '營業淨利(淨損)' == row[0]:
            income_statement_dto.oi = int(float(row[1]))
        # after 102
        elif '營業外收入及支出合計' in row[0]:
            income_statement_dto.nor = int(float(row[1]))
        # before 102
        elif '營業外收入及利益' in row[0]:
            income_statement_dto.nor += int(float(row[1]))
        # before 102
        elif '營業外費用及損失' in row[0]:
            income_statement_dto.nor -= int(float(row[1]))
        elif '稅前淨利（淨損）' == row[0] or \
                '繼續營業單位稅前淨利(淨損)' == row[0]:
            income_statement_dto.btax = int(float(row[1]))
        elif '本期淨利（淨損）' == row[0] or \
                '本期淨利(淨損)' == row[0]:
            income_statement_dto.ni = int(float(row[1]))
        elif '基本每股盈餘' in row[0]:
            income_statement_dto.eps = float(row[1])
