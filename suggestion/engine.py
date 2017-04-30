import pymongo
import numpy as np
from sklearn.cluster import DBSCAN
from hacreem.settings import MONGO_DB_PORT, MONGO_DB_IP


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


def get_suggested_business(lat, long):
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['retailer']
    cluster = mydb.cluster_feature.find({
        'area': {'$geoIntersects': {'$geometry': {'type': 'Point', 'coordinates': [lat, long]}}}
    }).limit(1)
    return list(cluster)


def make_feature_location():
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['retailer']
    data = list(mydb.retailer_logs.find())
    result = []
    for d in data:
        result.append({
            'location': {
                'type': 'Point',
                'coordinates': [d['retailer_long'], d['retailer_lat']]
            },
            'city': 'Karachi',
            'type': 'business',
            'business_type': d['retailer_category'],
            'name': d['retailer_name'],
            'ad': d['retailer_name'].split(' ')[0] + " has 30% discount! Avail Now"

        })
    mydb.retailer_feature.insert_many(result)


def make_dbscan_model():
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['retailer']
    data = list(mydb.retailer_feature.find(None, {'ad': 1, 'location.coordinates': 1}))
    location = []
    for d in data:
        location.append(d['location']['coordinates'])

    db = DBSCAN(eps=1 / 6371., min_samples=5, algorithm='ball_tree', metric='haversine').fit(np.radians(location))

    clusters = dict()
    for i in xrange(len(db.labels_)):
        if clusters.get(str(db.labels_[i])) is None:
            clusters[str(db.labels_[i])] = dict(ads=[], locations=[])
            clusters[str(db.labels_[i])]['ads'].append(data[i]['ad'])
            clusters[str(db.labels_[i])]['locations'].append(location[i])
        else:
            clusters[str(db.labels_[i])]['ads'].append(data[i]['ad'])
            clusters[str(db.labels_[i])]['locations'].append(location[i])

    clusters.pop('-1', None)
    cluster_array = []
    for key, item in clusters.items():
        item['locations'].sort(key=lambda x: x[0])
        item['locations'].append(item['locations'][0])
        cluster_array.append({
            'area': {
                "type": "Polygon",
                "coordinates": [item['locations']]
            },
            'cluster_id': key,
            'ads': item['ads']
        })
    mydb.cluster_feature.insert_many(cluster_array)


def product_recommendation_engine(user_id, lng, lat, gender, age_range, cab_service):
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['hacareem']
    retailer_info = get_suggested_business(lng, lat)
    # dummy
    user_info = {'gender': 'Male', 'age_range': 31, 'user_id': 'bbd525a64d', 'cab_service': 'Business'}
    recommendations = []
    required_recom_fields = ['retailer_product_name', 'retailer_product_target_age',
                             'retailer_product_target_gender', 'retailer_product_discount']

    for retailer in retailer_info:
        if retailer['ride_type'] == user_info['cab_service']:
            if retailer['retailer_product_target_gender'] == user_info['gender']:
                ret_age_lst = retailer['retailer_product_target_age'].split('-')
                if user_info['age_range'] in list(xrange(ret_age_lst[0], ret_age_lst[0] + 1)):
                    shortlisted_dict = {k: v for k, v in retailer.items() if k in required_recom_fields}
                    recommendations.append(shortlisted_dict)
    return recommendations
