import argparse
import datetime
import time

from stock_crawler import StockCrawler
from db_manage import DBManage


def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-sd", "--start",
        dest="start_date",
        type=str,
        help="The start date you want to search from"
    )
    parser.add_argument(
        "-ed", "--end",
        dest="end_date",
        type=str,
        help="The end date you want to search to"
    )
    parser.add_argument(
        "-l", "--latest",
        dest="to_latest",
        action='store_true',
        default=False,
        help="Update to latest info"
    )

    return parser.parse_args()


def get_start_end_date(pv_collection, arg_options):
    start_date = arg_options.start_date
    end_date = arg_options.end_date

    ticker_date_info = pv_collection \
        .find_one({'ticker': ticker})['date_info']
    if len(ticker_date_info) <= 0:
        return[arg_options.start_date, arg_options.end_date]
    ticker_date_info.sort(key=lambda x: x['date'])

    db_first_date = ticker_date_info[0]['date']
    db_last_date = ticker_date_info[len(ticker_date_info) - 1]['date']

    if arg_options.to_latest:
        print(db_last_date)
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

for ticker in stock_crawler.stocks_list:
    pv_collection = db_manage.get_collection(db_manage.collection_name)

    for attempt in range(0, 3):
        try:
            ticker_obj = pv_collection.find_one({'ticker': ticker})
            if ticker_obj and len(ticker_obj['date_info']) > 0:
                [start_date, end_date] = get_start_end_date(
                    pv_collection, arg_options)
                if not start_date and not end_date:
                    continue

                dto = stock_crawler.get_price_and_vol(
                    ticker, start_date, end_date)
                db_manage.update_pricevolume(dto)
            else:
                dto = stock_crawler.get_price_and_vol(
                    ticker, arg_options.start_date, arg_options.end_date)
                db_manage.insert_pricevolume(dto)
        except Exception as e:
            print(f'Error: {e}')
            print(f'Ticker {ticker} wait for 5(s) and retry')
            time.sleep(10)
            continue
        else:
            break
    else:
        raise Exception('Retried 3 times still fail')

print('All data update completed')
