import pymongo
import numpy as np
from sklearn.cluster import DBSCAN 
from hacreem.settings.constants import *

def dbscan_labels():

	client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
	mydb = client['hacareem']

	List_ = []

	cursor = list(mydb.ride_logs.find())
	for doc in cursor:
		List_.append([doc['pick_up_lat'], doc['pick_up_lng']])

	db = DBSCAN(eps=0.5/6371., min_samples=5, algorithm = 'ball_tree', metric = 'haversine').fit(np.radians(List_))	
	
	List_ = []

	for doc, cursor in zip(cursor, list(db.labels_)):
		output_dict = {'cursor':cursor}
		for key in ['pick_up_lat', 'pick_up_lng']:
			output_dict[key] = doc[key]
		List_.append(output_dict)

	return List_