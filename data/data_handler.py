import pymongo
import pandas as pd


class DataHandler(object):
    @classmethod
    def update_user_log_db(cls, fname='./dataset/large.csv', chunk_size=1000):
        for chunk in pd.read_csv(fname, chunksize=chunk_size, low_memory=True):
            print chunk.shape()
