from _collections import defaultdict
from matplotlib.cbook import Null
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from elasticsearch import Elasticsearch
import sys
import math
import json
import findspark

findspark.init()
topic = b'yelp-stream'


# DataType and Index for Elasticsearch
dType = "resturant"
yelpIndex = "yelpreco"
conf_param = ""+yelpIndex + '/' + dType
es = Elasticsearch()
if not es.indices.exists(yelpIndex):  # create if the index does not exist
    es.indices.create(yelpIndex)

category_user = Null

    
# mapping data for elasticsearch    
mapping = {
            dType: {
                "properties": {
                "businessId":{"type": "string"},
                "name":{"type": "string"},
                "full_address":{"type":"string"},
                "categories":{"type": "string"},"stars":{"type": "string"},
                "location":{"type": "geo_point", "index": "not_analyzed"},
                }
            }
        }

es.indices.put_mapping(index=yelpIndex, doc_type=dType, body=mapping)


def clearElasticSearch():
    #print "Inside readElasticSearch" 
    jcount = es.count(yelpIndex, dType, q="*:*")
    if jcount > 0:
        es.delete_by_query(yelpIndex, True,dType,q="*:*")
        es.indices.refresh(yelpIndex)

def readElasticSearch(sc): 
    #print "Inside readElasticSearch"
    rdd = sc.newAPIHadoopRDD(inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
                            keyClass="org.apache.hadoop.io.NullWritable",
                            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                            conf={ "es.resource" : "yelpraw/resturant" })    
    return rdd
    
def location_recommender(yelpData):
    global category_user
    #print "Inside location_recommender"
    user_location = (36.1027496,-115.1686673)
    #print yelpData
    if yelpData == None:
        return
    rcvd_data = yelpData[1]
    #print "Rcvd Data: ",rcvd_data
    #category_user = "Restaurants"
    category_data = rcvd_data.get("categories")
    data_Cat = str(category_data).strip('[]')
    if(category_user == "" or category_user in data_Cat):
        #print"Category found",category_user
        destination = (float(rcvd_data.get("latitude")), float(rcvd_data.get("longitude")))
        dist_location = distance(user_location, destination)
        #print "distance from the resturant is : ",dist_location
        if(dist_location < 5):
            return rcvd_data

def remap_es(rec):
    
    if rec is None:
        return
    rec = rec[1]
    location = rec["latitude"] + "," + rec["longitude"]
            
    content = ('key', {"businessId":rec["Business_Id"],
                    "name":rec["name"],
                     "full_address":rec["full_address"],
                     "categories": rec["categories"],"stars":rec["stars"],
                     "location":location})
    
    return content

def printResult(sortedD):
    if sortedD is None:
        return None
    for item in sortedD:
        rec = item[1]
        result = u' '.join((rec["name"], rec["full_address"], rec["stars"])).encode('utf-8').strip()
        print result

keyDict = defaultdict()
def copyUniqueData(sortedD, count):
    
    resultList = []
    for item in sortedD:
        rec = item[1]
        if not rec["Business_Id"] in keyDict:
            keyDict[rec["Business_Id"]] = "Present"
            resultList.append(item)
            if len(keyDict) == count:
                return resultList
    return resultList
def main():
    conf = SparkConf().setMaster("local[2]").setAppName("YelpRecommender")
    sc = SparkContext(conf=conf)
    rdd_data = readElasticSearch(sc)
    parsed_mapped_data = rdd_data.filter(location_recommender)
    sorted_data = parsed_mapped_data.top(150, key=lambda a: a[1]["stars"])
    topn_data = copyUniqueData(sorted_data, 5)
    printResult(topn_data)
    clearElasticSearch()
    sorted_rdd = sc.parallelize(topn_data)
    es_data = sorted_rdd.map(remap_es)
    es_data.saveAsNewAPIHadoopFile(path='-',
                               outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                               keyClass="org.apache.hadoop.io.NullWritable",
                               valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                               conf={ "es.resource" :  "yelpreco/resturant"})    
    

def distance(origin, destination):
    lat1, lon1 = origin
    lat2, lon2 = destination
    radius = 6371 # km
    dlat = math.radians(lat2-lat1)
    dlon = math.radians(lon2-lon1)
    a = math.sin(dlat/2) * math.sin(dlat/2) + math.cos(math.radians(lat1)) \
        * math.cos(math.radians(lat2)) * math.sin(dlon/2) * math.sin(dlon/2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    d = radius * c
    return d

if __name__ == '__main__':
    #category_user = raw_input("Please enter the type of restaurant you are looking for: ")
    if len(sys.argv) == 2:
        category_user = sys.argv[1]
        print "User Given category",category_user
    else:
        category_user = ""
        print "No user preferred category is given. Show top 5 trending resturants"
    main()