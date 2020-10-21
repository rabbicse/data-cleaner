#! -*- coding: utf-8 -*-

import csv
import datetime
import glob
import logging
import os
import re
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
from multiprocessing import Semaphore, Value

import pymongo
from pymongo import DeleteOne
from sqlalchemy import create_engine, Column, Integer, UnicodeText, Unicode, func, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

MONGO_URI = 'mongodb://localhost:27017'
MONGO_DATABASE = 'data'
MONGO_COLLECTION_NAME = 'items'

logger = logging.getLogger(__name__)


def setup_logger():
    logger.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # create file handler which logs even debug messages
    fh = RotatingFileHandler('csv_cleaner.log', maxBytes=10 * 1024 * 1024, backupCount=5)
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)

    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(fh)
    logger.addHandler(ch)


class CsvToDbConverter:
    __total = Value('i', 0)
    __lock = Semaphore()

    def __init__(self, input_csv=None):
        if input_csv:
            self.__input_csv = input_csv + '.csv' if not str(input_csv).lower().endswith('.csv') else input_csv
        else:
            self.__input_csv = None

        # set mongo credentials
        self.mongo_uri = MONGO_URI
        self.mongo_db = MONGO_DATABASE
        self.mongo_collection = MONGO_COLLECTION_NAME

    def __enter__(self):
        self.__init()

        # initialize mongodb client
        self.client = pymongo.MongoClient(self.mongo_uri, maxPoolSize=50000)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.mongo_collection]
        # self.collection.create_index([("Name", pymongo.TEXT), ("full_address", pymongo.TEXT)], name='search_index',
        #                              unique=True)
        # self.collection.create_index("Name")
        # self.collection.create_index("full_address")
        self.collection.create_index('hash')
        self.collection.create_index('file')

        self.pipeline = [
            {
                '$group': {
                    '_id': {'hash': '$hash'},
                    'uniqueIds': {'$addToSet': '$_id'},
                    'files': {'$addToSet': '$file'},
                    'count': {'$sum': 1},
                }
            },

            {'$match':
                 {'count':
                      {'$gt': 1}
                  }
             }
        ]
        # self.collection.aggregate(pipeline)
        return self

    def __init(self):
        hdr = [('Name', 'Name'),
               ('Website', 'Website'),
               ('Type', 'Type'),
               ('subtypes', 'subtypes'),
               ('Phone', 'Phone'),
               ('full_address', 'full_address'),
               ('borough', 'borough'),
               ('street', 'street'),
               ('city', 'city'),
               ('postal_code', 'postal_code'),
               ('country', 'country'),
               ('latitude', 'latitude'),
               ('longitude', 'longitude'),
               ('time_zone', 'time_zone'),
               ('plus_code', 'plus_code'),
               ('rating', 'rating'),
               ('reviews', 'reviews'),
               ('reviews_link', 'reviews_link'),
               ('photo', 'photo'),
               ('working_hours_old_format', 'working_hours_old_format'),
               ('price_range', 'price_range'),
               ('posts', 'posts'),
               ('verified', 'verified'),
               ('reserving_table_link', 'reserving_table_link'),
               ('booking_appointment_link', 'booking_appointment_link'),
               ('location_link', 'location_link'),
               ('email', 'email'),
               ('email2', 'email2'),
               ('twitter', 'twitter'),
               ('linkedin', 'linkedin'),
               ('facebook', 'facebook'),
               ('instagram', 'instagram'),
               ('google_plus', 'google_plus'),
               ('skype', 'skype'),
               ('telegram', 'telegram'),
               ('site_generator', 'site_generator'),
               ('site_title', 'site_title'),
               ('site_description', 'site_description'),
               ('site_keywords', 'site_keywords')]

        self.__csv_header = OrderedDict(hdr)
        self.__field_names = list(self.__csv_header.keys())

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.__input_csv:
            logger.info('=== Finish processing: {} ==='.format(self.__input_csv))

        self.client.close()
        del self.client
        del self

    def process_data(self):
        try:
            logger.info('=== Start processing: {} ==='.format(self.__input_csv))
            if os.path.exists(self.__input_csv):
                with open(self.__input_csv, 'r', errors='ignore') as f:
                    reader = csv.reader(f, quoting=csv.QUOTE_ALL)
                    index = 0
                    buffer = []
                    for row in reader:
                        try:
                            if index == 0 or not row:
                                continue

                            if len(row) < 13:
                                logger.warning('There are less than 13 columns!')
                                continue

                            # old code
                            # Create a zip object from two lists
                            zip_data = zip(self.__field_names, row)
                            # Create a dictionary from zip object
                            data_dict = dict(zip_data)

                            nm = data_dict['Name'].lower().strip() if 'Name' in data_dict else ''
                            add = data_dict['full_address'].lower().strip() if 'full_address' in data_dict else ''
                            data_dict['hash'] = nm + add
                            data_dict['file'] = self.__input_csv

                            # try:
                            #     # if any([b for b in buffer if data_dict['Name'] == b['Name'] and
                            #     #                              data_dict['full_address'] == b['full_address']]):
                            #     #     logger.warning('item already exists!')
                            #     #     continue
                            #
                            #     query = {'Name': re.compile(data_dict['Name'], re.IGNORECASE),
                            #              'full_address': re.compile(data_dict['full_address'], re.IGNORECASE)}
                            #     # q_data = self.collection.find(query)
                            #     # if q_data and q_data.count() > 0:
                            #     #     logger.warning('item inside database!')
                            #     #     continue
                            #
                            #     q_data = self.collection.find_one(query)
                            #     if q_data:
                            #         logger.warning('item inside database!')
                            #         continue
                            # except:
                            #     pass

                            # new code
                            buffer.append(data_dict)
                            if len(buffer) % 1000 == 0:
                                # # todo:
                                # for document in self.collection.aggregate(self.pipeline):
                                #     it = iter(document['uniqueIds'])
                                #     next(it)
                                #     for id in it:
                                #         buffer.append(DeleteOne({'_id': id}))

                                self.collection.insert_many(buffer)

                                buffer = []
                        except Exception as ex:
                            logger.error('Error processing each row: {}'.format(ex))
                        finally:
                            index += 1
        except Exception as x:
            logger.error('Error when process data: {}'.format(x))

    def remove_duplicates(self):
        try:
            logger.info('====== Removing duplicates =======')
            buffer = []
            for document in self.collection.aggregate(self.pipeline):
                it = iter(document['uniqueIds'])
                next(it)
                for id in it:
                    buffer.append(DeleteOne({'_id': id}))

            self.collection.bulk_write(buffer)
        except Exception as x:
            logger.error('Error when remove duplicates. Details: {}'.format(x))


if __name__ == '__main__':
    setup_logger()

    input_directory = './csv_data'  # input('Please specify Input csv: ')
    for input_file in glob.glob('{}/*.csv'.format(input_directory)):
        logger.info('Processing CSV: {}'.format(input_file))
        with CsvToDbConverter(input_file) as converter:
            converter.process_data()

            converter.remove_duplicates()
