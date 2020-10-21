# -*- coding: utf-8 -*-
import logging
from collections import OrderedDict
from logging.handlers import RotatingFileHandler

from pymongo import MongoClient
import csv
import time

logger = logging.getLogger(__name__)

MONGO_URI = 'mongodb://localhost:27017'
MONGO_DATABASE = 'data'
MONGO_COLLECTION_NAME = 'items'


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


class MongoToFile:
    def __init__(self, output_csv):
        if output_csv:
            self.__output_csv = output_csv + '.csv' if not str(output_csv).lower().endswith('.csv') else output_csv
        else:
            self.__output_csv = None

        # set mongo credentials
        self.mongo_uri = MONGO_URI
        self.mongo_db = MONGO_DATABASE
        self.mongo_collection = MONGO_COLLECTION_NAME

    def __enter__(self):
        # initialize mongodb client
        self.client = MongoClient(self.mongo_uri, maxPoolSize=50000)
        self.db = self.client[self.mongo_db]
        self.collection = self.db[self.mongo_collection]

        hdr = [
            ('Name', 'Name'),
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
        self.csv_header = OrderedDict(hdr)
        self.field_names = list(self.csv_header.keys())

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        del self

    def export_to_csv(self):
        # start time of script
        start_time = time.time()

        # make an API call to the MongoDB server
        cursor = self.collection.find()

        # extract the list of documents from cursor obj
        mongo_docs = list(cursor)

        # restrict the number of docs to export
        # mongo_docs = mongo_docs[:50]  # slice the list
        logger.info("total docs: {}".format(len(mongo_docs)))

        # create an empty DataFrame for storing documents
        # docs = pandas.DataFrame(columns=[])

        with open(self.__output_csv, 'w+', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=self.field_names, quoting=csv.QUOTE_ALL)
            writer.writerow(self.csv_header)

            # iterate over the list of MongoDB dict documents
            for num, doc in enumerate(mongo_docs):

                # item = {
                #     'Name': doc['Name'] if 'Name' in doc else '',
                #     'Website': doc['Website'] if 'Website' in doc else '',
                #     'Type': doc['Type'] if 'Type' in doc else '',
                #     'subtypes': doc['subtypes'] if 'subtypes' in doc else '',
                #     'Phone': doc['Phone'] if 'Phone' in doc else '',
                #     'full_address': doc['full_address'] if 'full_address' in doc else '',
                #     'borough': doc['borough'] if 'borough' in doc else '',
                #     'street': doc['street'] if 'street' in doc else '',
                #     'city': doc['city'] if 'city' in doc else '',
                #     'postal_code': doc['postal_code'] if 'postal_code' in doc else '',
                #     'country': doc['country'] if 'country' in doc else '',
                #     'latitude': doc['latitude'] if 'latitude' in doc else '',
                #     'longitude': doc['longitude'] if 'longitude' in doc else '',
                #     'time_zone': doc['time_zone'] if 'time_zone' in doc else '',
                #     'plus_code': doc['plus_code'] if 'plus_code' in doc else '',
                #     'rating': doc['rating'] if 'rating' in doc else '',
                #     'reviews': doc['reviews'] if 'reviews' in doc else '',
                #     'reviews_link': doc['reviews_link'] if 'reviews_link' in doc else '',
                #     'photo': doc['photo'] if 'photo' in doc else '',
                #     'working_hours_old_format': doc[
                #         'working_hours_old_format'] if 'working_hours_old_format' in doc else '',
                #     'price_range': doc['price_range'] if 'price_range' in doc else '',
                #     'posts': doc['posts'] if 'posts' in doc else '',
                #     'verified': doc['verified'] if 'verified' in doc else '',
                #     'reserving_table_link': doc['reserving_table_link'] if 'reserving_table_link' in doc else '',
                #     'booking_appointment_link': doc[
                #         'booking_appointment_link'] if 'booking_appointment_link' in doc else '',
                #     'location_link': doc['location_link'] if 'location_link' in doc else '',
                #     'email': doc['email'] if 'email' in doc else '',
                #     'email2': doc['email2'] if 'email2' in doc else '',
                #     'twitter': doc['twitter'] if 'twitter' in doc else '',
                #     'linkedin': doc['linkedin'] if 'linkedin' in doc else '',
                #     'facebook': doc['facebook'] if 'facebook' in doc else '',
                #     'instagram': doc['instagram'] if 'instagram' in doc else '',
                #     'google_plus': doc['google_plus'] if 'google_plus' in doc else '',
                #     'skype': doc['skype'] if 'skype' in doc else '',
                #     'telegram': doc['telegram'] if 'telegram' in doc else '',
                #     'site_generator': doc['site_generator'] if 'site_generator' in doc else '',
                #     'site_title': doc['site_title'] if 'site_title' in doc else '',
                #     'site_description': doc['site_description'] if 'site_description' in doc else '',
                #     'site_keywords': doc['site_keywords'] if 'site_keywords' in doc else '',
                # }

                item = {}
                for field in self.field_names:
                    item[field] = doc[field] if field in doc else ''
                writer.writerow(item)

                # convert ObjectId() to str
                # doc["_id"] = str(doc["_id"])

                # get document _id from dict
                # doc_id = doc["_id"]

                # create a Series obj from the MongoDB dict
                # series_obj = pandas.Series(doc, name=doc_id)

                # append the MongoDB Series obj to the DataFrame obj
                # docs = docs.append(series_obj)

                # only print every 10th document
                if num % 10000 == 0:
                    logger.info('Writing: {} rows.'.format(num))
                    f.flush()

        logger.info("\n\n==== time elapsed: {}".format(time.time() - start_time))


if __name__ == '__main__':
    setup_logger()
    # mode = int(input('Please specify Mode (1 for db insert, 2 for csv export!): '))

    output_file = './output.csv'  # input('Please specify Input csv: ')
    with MongoToFile(output_file) as converter:
        converter.export_to_csv()
