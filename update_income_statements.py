import argparse
import datetime
import time

from stock_crawler import StockCrawler
from db_manage import DBManage


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--start',
        dest='start_season',
        type=str,
        help='The start year and season you want to search from, '
        'format should be "Y-S"'
    )
    parser.add_argument(
        '-e', '--end',
        dest='end_season',
        type=str,
        help='The end year and season you want to search to, '
        'format should be "Y-S"'
    )
    parser.add_argument(
        '-l', '--latest',
        dest='to_latest',
        action='store_true',
        default=False,
        help='Update to latest info'
    )

    return parser.parse_args()


def get_start_end_season(ticker_obj, arg_options):
    start_season_str = arg_options.start_season
    end_season_str = arg_options.end_season

    [start_year, start_season] = start_season_str.split('-')
    [end_year, end_season] = end_season_str.split('-')

    if not ticker_obj or len(ticker_obj['income_statements']) <= 0:
        return[start_year, start_season, end_year, end_season]
    ticker_income_statements = ticker_obj['income_statements']
    ticker_income_statements.sort(key=lambda x: (x['year'], x['season']))

    [db_first_year, db_first_season, db_last_year, db_last_season] = \
        [
            ticker_income_statements[0]['year'],
            ticker_income_statements[0]['season'],
            ticker_income_statements[
                len(ticker_income_statements) - 1
            ]['year'],
            ticker_income_statements[
                len(ticker_income_statements) - 1
            ]['season']
    ]

    if arg_options.to_latest:
        print(f'The latest date in db is '
              f'year: {db_last_year}, season: {db_last_season}')
        cur_datetime = datetime.date.today()
        cur_year = cur_datetime.year
        cur_season = get_season(cur_datetime.month)
        return [db_last_year, db_last_season, cur_year, cur_season]

    encode_start_season = int(str(start_year) + str(start_season))
    encode_end_season = int(str(end_year) + str(end_season))
    encode_db_first_season = int(str(db_first_year) + str(db_first_season))
    encode_db_last_season = int(str(db_last_year) + str(db_last_season))
    if encode_end_season > encode_db_last_season:
        if encode_start_season >= encode_db_first_season:
            start_year = db_last_year
            start_season = db_last_season
    elif encode_start_season < encode_db_first_season:
        if encode_end_season <= encode_db_last_season:
            end_year = db_first_year
            end_season = db_first_season
    else:
        return [None, None, None, None]

    return [start_year, start_season, end_year, end_season]


def get_season(month):
    if month >= 1 and month <= 3:
        return 1
    elif month >= 4 and month <= 6:
        return 2
    elif month >= 7 and month <= 9:
        return 3
    else:
        return 4


arg_options = arg_parse()
stock_crawler = StockCrawler()
db_manage = DBManage()

is_collection = db_manage.get_collection(db_manage.collection_name)
for ticker in stock_crawler.stocks_list:
    for attempt in range(0, 3):
        try:
            ticker_obj = is_collection.find_one({'ticker': ticker})
            [start_year, start_season, end_year, end_season] = \
                get_start_end_season(ticker_obj, arg_options)

            if not start_year or not end_year \
                    or not start_season or not end_season:
                break

            dto = stock_crawler.get_income_statements_dto(
                ticker, start_year, start_season, end_year, end_season)
            if ticker_obj:
                db_manage.update_income_statements(dto)
            else:
                db_manage.insert_stock(dto)
        except Exception as e:
            print(f'Error: {e}')
            print(f'Ticker {ticker} wait for 5(s) and retry')
            time.sleep(10)
            continue
        else:
            break
    else:
        raise Exception('Retried 3 times still fail')

print('All income statements update completed')
