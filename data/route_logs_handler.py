import random
import pymongo
import pandas as pd

from hacreem.settings.settings import DJANGO_ROOT
from hacreem.settings.constants import *

client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
mydb = client['hacareem']

class RouteLogsHandler(object):

    DATA_ROOT = '%s/data/dataset'%(DJANGO_ROOT)

    @classmethod
    def update_user_log_db(cls, fname = 'large.csv', chunk_size = 1000):

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

        bulk = mydb.ride_logs.initialize_unordered_bulk_op()

        for user in chunk.to_dict(orient='records'):
            user['ride_type'] = 'Business' if (
                user['user_id'] == 'bbd525a64d') else random.choice(service)

            r_ids = []

            for direction in ['pick_up', 'drop_off']:
                r_ids.append(
                    '%f_%f'%(user['%s_lat'%direction], user['%s_lng'%direction]))

            user['retailer_ids'] = r_ids

            bulk.insert(user)

        return bulk