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

    db = DBSCAN(eps=0.5 / 6371., min_samples=5, algorithm='ball_tree', metric='haversine').fit(np.radians(location_list))

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


def make_feature_location():
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['retailer']
    data = list(mydb.retailer_logs.find())
    result = []
    for d in data:
        result.append({
            'type': 'Feature',
            'geometry': {
                'type': 'Point',
                'coordinates': [d['retailer_lat'], d['retailer_long']]
            },
            'properties': {
                'city': 'Karachi',
                'type': 'business',
                'business_type': d['retailer_category'],
                'name': d['retailer_name'],
                'ad': d['retailer_name'].split(' ')[0] + " has 30% discount! Avail Now"
            }
        })
    mydb.retailer_feature.insert_many(result)


def make_dbscan_model():
    client = pymongo.MongoClient(MONGO_DB_IP, MONGO_DB_PORT)
    mydb = client['retailer']
    data = list(mydb.retailer_feature.find(None, {'properties.ad': 1, 'geometry.coordinates': 1}))
    location = []
    for d in data:
        location.append(d['geometry']['coordinates'])

    db = DBSCAN(eps=1 / 6371., min_samples=5, algorithm='ball_tree', metric='haversine').fit(np.radians(location))

    clusters = dict()
    for i in xrange(len(db.labels_)):
        if clusters.get(str(db.labels_[i])) is None:
            clusters[str(db.labels_[i])] = dict(ads=[], locations=[])
            clusters[str(db.labels_[i])]['ads'].append(data[i]['properties']['ad'])
            clusters[str(db.labels_[i])]['locations'].append(location[i])
        else:
            clusters[str(db.labels_[i])]['ads'].append(data[i]['properties']['ad'])
            clusters[str(db.labels_[i])]['locations'].append(location[i])

    clusters.pop('-1', None)
    cluster_array = []
    for key, item in clusters.items():
        item['locations'].append(item['locations'][0])
        cluster_array.append({
            'type': 'Feature',
            'geometry': {
                "type": "Polygon",
                "coordinates": item['locations']
            },
            'properties': {
                'cluster_id': key,
                'ads': item['ads']
            }
        })
    mydb.cluster_feature.insert_many(cluster_array)
