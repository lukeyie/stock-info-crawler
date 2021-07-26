import argparse
import datetime
import time

from stock_crawler import StockCrawler
from db_manage import DBManage


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '-s', '--start',
        dest='start_date',
        type=str,
        help='The start date you want to search from, '
        'format should be "yyyy-mm-dd"'
    )
    parser.add_argument(
        '-e', '--end',
        dest='end_date',
        type=str,
        help='The end date you want to search to, '
        'format should be "yyyy-mm-dd"'
    )
    parser.add_argument(
        '-l', '--latest',
        dest='to_latest',
        action='store_true',
        default=False,
        help='Update to latest info'
    )

    return parser.parse_args()


def get_start_end_date(ticker_obj, arg_options):
    start_date = arg_options.start_date
    end_date = arg_options.end_date

    if not ticker_obj or len(ticker_obj['date_info']) <= 0:
        return[arg_options.start_date, arg_options.end_date]
    ticker_date_info = ticker_obj['date_info']
    ticker_date_info.sort(key=lambda x: x['date'])

    db_first_date = ticker_date_info[0]['date']
    db_last_date = ticker_date_info[len(ticker_date_info) - 1]['date']

    if arg_options.to_latest:
        print(f'The latest date in db is {db_last_date}')
        return [db_last_date,
                str(datetime.date.today() + datetime.timedelta(1))]

    if end_date > db_last_date:
        start_date = db_last_date \
            if start_date >= db_first_date else start_date
    elif start_date < db_first_date:
        end_date = db_first_date \
            if end_date <= db_last_date else end_date
    else:
        return [None, None]

    return [start_date, end_date]


arg_options = arg_parse()
stock_crawler = StockCrawler()
db_manage = DBManage()

pv_collection = db_manage.get_collection(db_manage.collection_name)
for ticker in stock_crawler.stocks_list:
    for attempt in range(0, 3):
        try:
            ticker_obj = pv_collection.find_one({'ticker': ticker})
            [start_date, end_date] = get_start_end_date(
                ticker_obj, arg_options)

            if not start_date or not end_date:
                break

            dto = stock_crawler.get_price_and_vol(
                ticker, start_date, end_date)

            if ticker_obj:
                db_manage.update_pricevolume(dto)
            else:
                db_manage.insert_stock(dto)

        except ValueError as v:
            print(v)
            time.sleep(120)
            continue
        except Exception as e:
            print(f'Error: {e}')
            print(f'Ticker {ticker} wait for 5(s) and retry')
            time.sleep(5)
            continue
        else:
            break
    else:
        raise Exception('Retried 3 times still fail')

print('All price volume update completed')
