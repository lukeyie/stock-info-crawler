import json
import time
from decimal import Decimal, ROUND_HALF_UP

from pymongo import MongoClient

from dto import StockDTO


class DBManage:
    __CREDENTIAL_FILE_PATH = '.\dbCredential.config'

    def __init__(self):
        input_file = open(self.__CREDENTIAL_FILE_PATH)
        self.__credential = json.load(input_file)
        self.collection_name = self.__credential['collection_name']
        self.__db_uri = self.__create_db_connection_url()
        self.__db_instance = self.__connect_db(self.__db_uri)

    def insert_stock(self, dto: StockDTO):
        collection = self.__db_instance[self.collection_name]

        print(f'Start insert ticker : {dto.ticker}')
        timer_start = time.time()
        collection.insert_one(dto.dict())
        timer_end = time.time()
        pass_time = Decimal(timer_end - timer_start).quantize(
            Decimal('.1'), rounding=ROUND_HALF_UP)
        print(f'Insert {dto.ticker} completed ({pass_time} s)')

    def update_pricevolume(self, dto: StockDTO):
        collection = self.__db_instance[self.collection_name]

        print(f'Start update ticker : {dto.ticker}')
        timer_start = time.time()
        date_info_list = []
        for date_info in dto.date_info:
            date_info_list.append(date_info.dict())
        collection.update_one(
            {'ticker': dto.ticker},
            {'$addToSet': {'date_info': {'$each': date_info_list}}},
            upsert=True
        )

        timer_end = time.time()
        pass_time = Decimal(timer_end - timer_start).quantize(
            Decimal('.1'), rounding=ROUND_HALF_UP)
        print(f'Update {dto.ticker} completed ({pass_time} s)')

    def update_income_statements(self, dto: StockDTO):
        collection = self.__db_instance[self.collection_name]

        print(f'Start update ticker : {dto.ticker}')
        timer_start = time.time()
        income_statements = []
        for income_statement in dto.income_statements:
            income_statements.append(income_statement.dict())
        collection.update_one(
            {'ticker': dto.ticker},
            {'$addToSet':
                {'income_statements': {'$each': income_statements}}
             },
            upsert=True
        )

        timer_end = time.time()
        pass_time = Decimal(timer_end - timer_start).quantize(
            Decimal('.1'), rounding=ROUND_HALF_UP)
        print(f'Update {dto.ticker} income statements completed '
              f'({pass_time} s)')

    def create_index_for_collection(self, collection,
                                    index_fields, index_name,
                                    is_accending=True):
        index_to_add = []
        for field in index_fields:
            index_to_add.append((field, 1 if is_accending else -1))

        self.__db_instance[collection] \
            .create_index(index_to_add, name=index_name)

    def get_collection(self, collection):
        return self.__db_instance[collection]

    def __create_db_connection_url(self):
        uri = f'mongodb+srv://{self.__credential["cluster_name"]}' \
              f'.1kypv.gcp.mongodb.net/{self.__credential["db_name"]}' \
            '?authSource=%24external&authMechanism=MONGODB-X509&' \
            'retryWrites=true&w=majority'
        return uri

    def __connect_db(self, uri):
        client = MongoClient(
            uri, tls=True,
            tlsCertificateKeyFile=f'{self.__credential["certificate_file_path"]}'
        )
        return client[self.__credential['db_name']]
