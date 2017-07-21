import json
from _collections import defaultdict
import math
import findspark
findspark.init()
from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
topic = b'yelp-stream'
from kafka import KafkaConsumer
from elasticsearch import Elasticsearch

# DataType and Index for Elasticsearch
dType = "resturant"
yelpIndex = "yelpraw"
conf_param = ""+yelpIndex + '/' + dType
es = Elasticsearch()

def remap_elastic(rec):
    content = ('key', rec)
    return content

def writeElasticSearch(rdd):
    print "Inside writeElasticSearch"    
    if rdd is None:
        return
    rdd.saveAsNewAPIHadoopFile(path='-',
                               outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                               keyClass="org.apache.hadoop.io.NullWritable",
                               valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                               conf={ "es.resource" :  "yelpraw/resturant"})


def main():
    conf = SparkConf().setMaster("local[2]").setAppName("YelpConsumer")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)

    kstream = KafkaUtils.createDirectStream(ssc, topics=['yelp-stream'],
                                            kafkaParams={"metadata.broker.list": 'localhost:9092'})

        
    parsed_json = kstream.map(lambda (k, v): json.loads(v))
    remapped_data = parsed_json.map(remap_elastic)
    
    remapped_data.foreachRDD(writeElasticSearch)
    ssc.start() 
    ssc.awaitTermination()
    
if __name__ == '__main__':
    main()