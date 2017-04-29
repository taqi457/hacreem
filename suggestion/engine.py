import pymongo
import numpy as np
from sklearn.cluster import DBSCAN
from hacreem.settings.constants import MONGO_DB_PORT, MONGO_DB_IP


def dbscan_labels():
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['hacareem']

    location_list = []

    cursor = list(mydb.ride_logs.find())
    for doc in cursor:
        location_list.append([doc['pick_up_lat'], doc['pick_up_lng']])

    db = DBSCAN(
    	eps=0.5 / 6371., min_samples=5, algorithm='ball_tree', metric='haversine', n_jobs=-1).fit(
    		np.radians(location_list))

    location_list = []

    for doc, cursor in zip(cursor, list(db.labels_)):
        output_dict = {'CLUSTOR': cursor}
        for key in ['pick_up_lat', 'pick_up_lng']:
            output_dict[key] = doc[key]
        location_list.append(output_dict)

def add_business(data):
    business_list = []
    for business in data:
        business_list.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [business['lat'], business['long']]
            },
            'properties': {
                'city': 'Karachi',
                'type': 'business',
                'business_type': business['restaurants']
            }
        })
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['hacareem']
    mydb.businesses.insert_many(business_list)


def get_suggested_business(lat, long):
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['hacareem']
    cluster = mydb.clusters.find({
        'area': {'$geoIntersects': {'$geometry': {'type': 'Point', 'coordinates': [lat, long]}}}
    }).limit(1)
    return list(cluster)


def product_recommendation_engine(user_id, lat, lng, gender, age_range, cab_service):
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['hacareem']

	retailer_info = get_suggested_business(lat, lng)
	
	#dummy
	user_info = {'gender': 'Male', 'age_range': 31, 'user_id': 'bbd525a64d', 'cab_service' : 'Business'}

	recommendations = []
	required_recom_fields = ['retailer_product_name', 'retailer_product_target_age', \
		'retailer_product_target_gender', 'retailer_product_discount']

	for retailer in retailer_info:
		if retailer['ride_type'] == user_info['cab_service']:
			if retailer['retailer_product_target_gender'] == user_info['gender']	
				ret_age_lst = retailer['retailer_product_target_age'].split('-')
				if user_info['age_range'] in list(xrange(
					ret_age_lst[0], ret_age_lst[0] + 1)):
					shortlisted_dict = {k:v for k,v in retailer.items() if k in required_recom_fields}
					recommendations.append(shortlisted_dict)

	return recommendations