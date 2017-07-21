import time
import json
import csv
from kafka import SimpleProducer, KafkaClient
from threading import Thread
from time import sleep

topic = b'yelp-stream'

csvPath = "/Users/atishpatra/SECOND_SEM_UTD/BigDataMgmt/Project/result_csvJoin/BusinessTip.csv"

kafka = KafkaClient("localhost:9092")
producer = SimpleProducer(kafka)

class Streamer (Thread):
    def __init__(self, threadID, name, counter):
        Thread.__init__(self)
        self.threadID = threadID
        self.name = name
    def run(self):
        print "Starting " + self.name
        with open("VegasResturantData.json") as json_data:
            lines = json_data.readlines()
            for line in lines:
                print "Sending data via kafka"
                sendkafka(line)
                sleep(1)
        print "Exiting " + self.name
        
def sendkafka(messages):
    #print"inside sendKafka"
    producer.send_messages(topic, messages.encode())
        
def csvToJson(csvPath):
    #print"inside csvToJson"        
    csvfile = open(csvPath, 'r')
    jsonfile = open('MergedYelpDataset.json', 'w')

    fieldnames = ("Business_Id","city","stars","categories","name","review_count","state","full_address","latitude","longitude","text")
    reader = csv.DictReader( csvfile, fieldnames)
    for row in reader:
        #print "row:", row
        json.dump(row, jsonfile)
        jsonfile.write('\n')
    csvfile.close()
    jsonfile.close()
    return

        
if __name__ == '__main__':
    #print"inside main"
    streamerTh = Streamer(1, "Streamer-Thread", 1)
    streamerTh.start()
    streamerTh.join()
    