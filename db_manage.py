import json
import time
from decimal import Decimal, ROUND_HALF_UP

from pymongo import MongoClient
from pydantic import ValidationError

from dto import PriceVolumeDTO


class DBManage:
    __CREDENTIAL_FILE_PATH = '.\dbCredential.config'
    __PRICE_VOLUME_COLLECTION = 'price_volume'

    def __init__(self):
        input_file = open(self.__CREDENTIAL_FILE_PATH)
        self.__credential = json.load(input_file)
        self.__db_uri = self.__create_db_connection_url()
        self.__db_instance = self.__connect_db(self.__db_uri)

    def insert_pricevolume(self, data):
        collection = self.__db_instance[self.__PRICE_VOLUME_COLLECTION]
        try:
            dto = PriceVolumeDTO.parse_obj(data).dict()
        except ValidationError as e:
            print('Price and Value can not transform to '
                  'PriceVolumeDTO successfully')
            raise e

        print(f'Start insert ticker : {dto["ticker"]}')
        timer_start = time.time()
        collection.insert_one(
            {'ticker': dto['ticker'], 'stock_name': dto['stock_name'],
                'date_info': dto['date_info']}
        )
        timer_end = time.time()
        pass_time = Decimal(timer_end - timer_start).quantize(
            Decimal('.1'), rounding=ROUND_HALF_UP)
        print(f'Insert {dto["ticker"]} completed ({pass_time} s)')

    def update_pricevolume(self, data):
        collection = self.__db_instance[self.__PRICE_VOLUME_COLLECTION]
        try:
            dto = PriceVolumeDTO.parse_obj(data).dict()
        except ValidationError:
            print('Price and Value can not transform to '
                  'PriceVolumeDTO successfully')

        print(f'Start update ticker : {dto["ticker"]}')
        timer_start = time.time()
        for date_info in dto['date_info']:
            collection.update_one(
                {'ticker': dto['ticker'], 'stock_name': dto['stock_name']},
                {'$addToSet': {'date_info': date_info}}, upsert=True
            )
        timer_end = time.time()
        pass_time = Decimal(timer_end - timer_start).quantize(
            Decimal('.1'), rounding=ROUND_HALF_UP)
        print(f'Update {dto["ticker"]} completed ({pass_time} s)')

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
