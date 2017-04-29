import random
import pymongo
import pandas as pd

from hacreem.settings.settings import DJANGO_ROOT
from hacreem.settings.constants import *

client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
mydb = client['hacareem']

class UsersHandler(object):

    DATA_ROOT = '%s/data/dataset'%(DJANGO_ROOT)

    @classmethod
    def update_users_db(cls, fname = 'users.csv', chunk_size = 1000):

        for chunk in pd.read_csv('%s/%s'%(
            cls.DATA_ROOT, fname), chunksize = chunk_size, low_memory = True):            
            
            bulk = mydb.user_logs.initialize_unordered_bulk_op()
            
            for user in chunk.to_dict(orient='records'):
                bulk.insert(user)

        result = bulk.execute()
        print 'Update or addition done\n'