import random
import pymongo
import pandas as pd

from hacreem.settings.settings import DJANGO_ROOT
from hacreem.settings.constants import *

client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
mydb = client['hacareem']

class RetailersHandler(object):

    DATA_ROOT = '%s/data/dataset'%(DJANGO_ROOT)

    @classmethod
    def update_retailer_db(cls, fname = 'retailer.csv', chunk_size = 1000):

        for chunk in pd.read_csv('%s/%s'%(
            cls.DATA_ROOT, fname), chunksize = chunk_size, low_memory = True):            
            
            bulk = cls.chunk_feature_enhancement(chunk)

        result = bulk.execute()
        print 'Update or addition done\n'

    @classmethod
    def chunk_feature_enhancement(cls, chunk):

        service_type = []
        default_service = 'Business'
        service = ['Business', 'Go+', 'Go', 'Tezz']

        bulk = mydb.retailer_logs.initialize_unordered_bulk_op()

        for retailer in chunk.to_dict(orient='records'):

            for key, default in {
                'retailer_category' : 'others', 'ride_type' : random.choice(
                    service)}.items():

                if str(retailer[key]) == 'nan':
                    retailer[key] = default

            for key in ['retailer_product_name', 'retailer_product_target_age', \
                'retailer_product_target_gender', 'retailer_product_discount']:

                if str(retailer[key]) == 'nan':
                    retailer[key] = None

            retailer['retailer_ids'] = '%f_%f'%(retailer['retailer_lat'], retailer['retailer_long'])
            
            bulk.insert(retailer)

        return bulk