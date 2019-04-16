from kafka import KafkaProducer
from time import sleep
import json, sys
import requests
import time
import os
import re
                  
def getData(url):
    jsonData = requests.get(url).json()
    data = []
    labels = {}
    index = 0
    id = 0

    if os.path.exists("./labels.csv"):
        file = open("labels.csv")
        for line in file:
            splitArr = line.split(",")
            labels[splitArr[0]] = int(splitArr[1])
            index += 1
        file.close()
    
    for i in range(len(jsonData["response"]['results'])):
        headline = jsonData["response"]['results'][i]['fields']['headline']
        bodyText = jsonData["response"]['results'][i]['fields']['bodyText']
        trailText = jsonData["response"]['results'][i]['fields']['trailText']
        label = jsonData["response"]['results'][i]['sectionName']
        if label not in labels:
            continue
            labels[label] = index
            index += 1
		
        headline = re.sub(r"[^a-zA-Z0-9]+", ' ', headline)
        bodyText = re.sub(r"[^a-zA-Z0-9]+", ' ', bodyText)
        trailText = re.sub(r"[^a-zA-Z0-9]+", ' ', trailText)
        headline = re.sub(r'[\n\r]+', '', headline)
        bodyText = re.sub(r'[\n\r]+', '', bodyText)
        toAdd = str(id) + '|'+headline+' '+bodyText+' '+trailText+'|'+str(labels[label])
        id += 1
        toAdd.replace("\r", "\t")
        data.append(toAdd.strip())

    file = open("labels.csv", "w")
    for key, value in labels.items():
        file.write(key+","+str(value)+"\n")
    file.close()
        
    return(data)

def publish_message(producer_instance, topic_name, value):
    try:
        key_bytes = bytes('foo', encoding='utf-8')
        value_bytes = bytes(value, encoding='utf-8')
        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully.')
    except Exception as ex:
        print('Exception in publishing message')
        print(str(ex))

def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=['localhost:9092'], api_version=(0, 10),linger_ms=10)
    
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer

if __name__== "__main__":
    key = ''
    fromDate = '2019-01-1'
    toDate = '2019-03-3'
    
    url = 'http://content.guardianapis.com/search?from-date='+ fromDate +'&to-date='+ toDate +'&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key='+key        
    all_news = getData(url)
	
    if len(all_news)>0:
        prod = connect_kafka_producer();
        for story in all_news:
            publish_message(prod, 'test', story)
            time.sleep(1)
        if prod is not None:
                prod.close()