import pymongo
import numpy as np
from sklearn.cluster import DBSCAN 
from hacreem.settings.constants import *

def dbscan_labels():

	client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
	mydb = client['hacareem']

	List_ = []

	for doc in mydb.ride_logs.find():
		List_.append([doc['pick_up_lat'], doc['pick_up_lng']])

	db = DBSCAN(eps=0.5/6371., min_samples=5, algorithm = 'ball_tree', metric = 'haversine').fit(np.radians(List_))	
	return db.labels_