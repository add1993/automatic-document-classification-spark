from kafka import KafkaProducer
from time import sleep
import json, sys
import requests
import time
import os
import re
import datetime

def getData(url, idx=0):
    jsonData = requests.get(url).json()
    data = []
    labels = {}
    index = 0
    
    if os.path.exists("./labels.csv"):
        file = open("labels.csv")
        for line in file:
            splitArr = line.split(",")
            labels[splitArr[0]] = int(splitArr[1])
            index += 1
        file.close()
    id = idx
    for i in range(len(jsonData["response"]['results'])):
        headline = jsonData["response"]['results'][i]['fields']['headline']
        bodyText = jsonData["response"]['results'][i]['fields']['bodyText']
        trailText = jsonData["response"]['results'][i]['fields']['trailText']
        headline += bodyText
        label = jsonData["response"]['results'][i]['sectionName']
        if label not in labels:
            labels[label] = index
            index += 1  
        headline = re.sub(r"[^a-zA-Z0-9]+", ' ', headline)
        bodyText = re.sub(r"[^a-zA-Z0-9]+", ' ', bodyText)
        trailText = re.sub(r"[^a-zA-Z0-9]+", ' ', trailText)
        headline = re.sub(r'[\n\r]+', '', headline)
        bodyText = re.sub(r'[\n\r]+', '', bodyText)
        toAdd = str(id) + '|'+headline+' '+bodyText+' '+trailText+'|'+str(labels[label])
        id = id + 1
        data.append(toAdd)

    file = open("newlabels.csv", "w")
    for key, value in labels.items():
        file.write(key+","+str(value)+"\n")
    file.close()

    return(data)

if __name__== "__main__":
    key = ""
    date1 = datetime.datetime(2015,8,1,12,4,5)
    date2 = datetime.datetime(2015,8,2,12,4,5)
    
    if not os.path.exists("./newdataset.csv"):
        f = open("dataset.csv", "a")
        f.write("id|text|label\n")
    else:
        f = open('dataset.csv', "a")
	
    example_idx = 0 
    for i in range(5):
        fromDate = date1.date().strftime("%Y-%m-%d")
        toDate = date2.date().strftime("%Y-%m-%d")
        url = 'http://content.guardianapis.com/search?from-date='+ fromDate +'&to-date='+ toDate +'&order-by=newest&show-fields=all&page-size=200&%20num_per_section=10000&api-key='+key
         
        print("Receiving data from : "+url)
        all_news = getData(url, example_idx)

        if len(all_news) > 0:
            for story in all_news:
                f.write(story+"\n")
            example_idx += len(all_news)
        date1 += datetime.timedelta(days=2)
        date2 += datetime.timedelta(days=2)

    f.close()
