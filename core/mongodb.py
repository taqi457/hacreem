import pymongo
from hacreem.settings.constants import MONGO_DB_IP, MONGO_DB_PORT


class MongoCollection(object):

    DATABASE = 'default'
    COLLECTION = 'default'
    ROUTINE_NAME = 'default'

    def __init__(self, *args, **kwargs):

        self.db_name = kwargs.get('ip', MONGO_DB_IP)
        self.db_port = kwargs.get('port', MONGO_DB_PORT)
        self.dbClient = pymongo.MongoClient(self.db_name,self.db_port)
        self.db_name = kwargs.get('database', self.DATABASE)
        self.collection_name = kwargs.get('collection', self.COLLECTION)
        self.collection = self.dbClient[self.db_name][self.collection_name]

    def get_data(self, filters={}, view=None, distinct=None, sort=None, ascending=True, limit=None, get_count=False):
        result = self.collection.find(filters, view)
        if sort is not None:
            if ascending:
                result = result.sort(sort, pymongo.ASCENDING)
            else:
                result = result.sort(sort, pymongo.DESCENDING)
        if limit is not None:
            result = result.limit(limit)
        if distinct is not None:
            result = result.distinct(distinct)
        result_list = list(result)

        for r in result_list:
            if '_id' in r:
                r['_id'] = str(r['_id'])
        if get_count:
            return result_list, result.count()
        else:
            return result_list

    def aggregate(self, filters={}):
        return list(self.collection.aggregate(filters))

    def insert(self, obj={}):
        return self.collection.insert(obj)
