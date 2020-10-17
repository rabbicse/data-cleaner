#! -*- coding: utf-8 -*-

import csv
import datetime
import glob
import logging
import os
from collections import OrderedDict
from logging.handlers import RotatingFileHandler
from multiprocessing import Semaphore, Value

from sqlalchemy import create_engine, Column, Integer, UnicodeText, Unicode, func, inspect
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

DB_HOST = '127.0.0.1'
DB_PORT = '3306'
DB_NAME = 'data'
DB_USERNAME = 'admin'
DB_PASS = 'password'

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


DeclarativeBase = declarative_base()


def db_connect():
    """Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance.
    """
    connection_str = 'mysql+mysqldb://{}:{}@{}:{}/{}?charset=utf8&use_unicode=1'.format(DB_USERNAME, DB_PASS, DB_HOST,
                                                                                        DB_PORT, DB_NAME)
    return create_engine(connection_str)


def create_tables(engine):
    """"""
    DeclarativeBase.metadata.create_all(engine)


def to_dict(obj, with_relationships=True):
    d = {}
    for column in obj.__table__.columns:
        if with_relationships and len(column.foreign_keys) > 0:
            # Skip foreign keys
            continue
        d[column.name] = getattr(obj, column.name)

    if with_relationships:
        for relationship in inspect(type(obj)).relationships:
            val = getattr(obj, relationship.key)
            d[relationship.key] = to_dict(val) if val else None
    return d


class Model(DeclarativeBase):
    """Sqlalchemy deals model"""
    __tablename__ = "csv_data"
    __table_args__ = {
        'mysql_charset': 'utf8'
    }

    def __init__(self, **kwargs):
        cls_ = type(self)
        for k in kwargs:
            if hasattr(cls_, k):
                setattr(self, k, kwargs[k])

    id = Column(Integer, primary_key=True)
    Name = Column(UnicodeText)
    Website = Column(UnicodeText)
    Type = Column(Unicode(255))
    subtypes = Column(UnicodeText)
    Phone = Column(Unicode(50))
    full_address = Column(UnicodeText)
    borough = Column(UnicodeText)
    street = Column(UnicodeText)
    city = Column(Unicode(100))
    postal_code = Column(Unicode(20))
    country = Column(Unicode(100))
    latitude = Column(UnicodeText)
    longitude = Column(UnicodeText)
    time_zone = Column(Unicode(100))
    plus_code = Column(Unicode(255))
    rating = Column(UnicodeText)
    reviews = Column(UnicodeText)
    reviews_link = Column(UnicodeText)
    photo = Column(UnicodeText)
    working_hours_old_format = Column(UnicodeText)
    price_range = Column(Unicode(255))
    posts = Column(UnicodeText)
    verified = Column(Unicode(255))
    reserving_table_link = Column(UnicodeText)
    booking_appointment_link = Column(UnicodeText)
    location_link = Column(UnicodeText)
    email = Column(UnicodeText)
    email2 = Column(UnicodeText)
    twitter = Column(UnicodeText)
    linkedin = Column(UnicodeText)
    facebook = Column(UnicodeText)
    instagram = Column(UnicodeText)
    google_plus = Column(UnicodeText)
    skype = Column(UnicodeText)
    telegram = Column(UnicodeText)
    site_generator = Column(UnicodeText)
    site_title = Column(UnicodeText)
    site_description = Column(UnicodeText)
    site_keywords = Column(UnicodeText)


class CsvToDbConverter:
    __total = Value('i', 0)
    __lock = Semaphore()

    def __init__(self, input_csv=None):
        self.session = None
        if input_csv:
            self.__input_csv = input_csv + '.csv' if not str(input_csv).lower().endswith('.csv') else input_csv
        else:
            self.__input_csv = None

    def __enter__(self):
        self.__init()
        engine = db_connect()
        engine.execution_options(stream_results=True)
        create_tables(engine)
        self.Session = sessionmaker(bind=engine)
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
        # else:
        #     logger.info('=== Export to csv: {} done! ==='.format(self.__output_csv))

        if self.session:
            self.session.close()
        del self

    def process_data(self):
        try:
            logger.info('=== Start processing: {} ==='.format(self.__input_csv))
            session = self.Session()
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

                            # db operation
                            # model = Model(**data_dict)
                            # session.add(model)
                            # session.commit()
                            # end old code

                            # new code
                            buffer.append(data_dict)
                            if len(buffer) % 10000 == 0:
                                session.bulk_insert_mappings(Model, buffer)
                                session.commit()
                                buffer = []

                            # row_data = ','.join([r.strip() for r in row if r])
                            # data_hash = hashlib.md5(row_data.encode('utf-8')).hexdigest()
                            # query = {'data_hash': data_hash}
                            # q_data = session.query(Model).filter_by(**query).first()
                            # if not q_data:
                            #     # Create a zip object from two lists
                            #     zip_data = zip(self.__field_names, row)
                            #     # Create a dictionary from zip object
                            #     data_dict = dict(zip_data)
                            #     data_dict['data_hash'] = data_hash
                            #
                            #     # db operation
                            #     model = Model(**data_dict)
                            #     session.add(model)
                            #     session.commit()
                            # else:
                            #     logger.warning('Data: {} already exists!'.format(data_hash))
                        except Exception as ex:
                            logger.error('Error processing each row: {}'.format(ex))
                        finally:
                            index += 1

                    session.bulk_insert_mappings(Model, buffer)
                    session.commit()
        except Exception as x:
            logger.error('Error when process data: {}'.format(x))
            if session:
                session.rollback()
        finally:
            if session:
                session.close()

    def export_to_csv(self, i=0, output_dir='./output_csv'):
        try:
            if not os.path.exists(output_dir):
                os.mkdir(output_dir)
            output_csv = datetime.datetime.now().strftime('{}/{}_output.csv'.format(output_dir, i))

            logger.info('Write start for: {}'.format(output_csv))
            with open(output_csv, mode='w+', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=self.__field_names, quoting=csv.QUOTE_ALL)
                writer.writerow(self.__csv_header)
                # session = self.Session()
                # records = session.query(Model).order_by(func.rand()).limit(100000).all()
                index = 0
                for record in self.get_results():
                    item = to_dict(record)
                    del item['id']

                    writer.writerow(item)
                    del record
                    del item
                    index += 1

                    if index % 100000 == 0:
                        break
        except Exception as x:
            logger.error('Error when export to csv from database.Error details: {}'.format(x))
        finally:
            logger.info('Write finish for: {}'.format(output_csv))
        #     if session:
        #         session.close()

    def query_db(self):
        try:
            self.session = self.Session()
            self.records = self.session.query(Model).order_by(func.rand()).yield_per(10000)
            # for record in records:
            #     yield record
        except Exception as x:
            logger.error(x)

    def get_results(self):
        try:
            for record in self.records:
                yield record
        except Exception as x:
            logger.error(x)


if __name__ == '__main__':
    setup_logger()
    mode = int(input('Please specify Mode (1 for db insert, 2 for csv export!): '))

    input_directory = './csv_data'  # input('Please specify Input csv: ')
    # for input_file in glob.glob('{}/*.csv'.format(input_directory)):
    #     logger.info('Processing CSV: {}'.format(input_file))
    #     with CsvToDbConverter(input_file) as converter:
    #         converter.process_data()

    if mode == 1:
        for input_file in glob.glob('{}/*.csv'.format(input_directory)):
            logger.info('Processing CSV: {}'.format(input_file))
            with CsvToDbConverter(input_file) as converter:
                converter.process_data()
    elif mode == 2:
        with CsvToDbConverter() as converter:
            converter.query_db()
            for i in range(1):
                converter.export_to_csv(i + 1)
