#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import datetime
import boto3
import json
from elasticsearch import Elasticsearch, RequestsHttpConnection
from requests_aws4auth import AWS4Auth


# In[ ]:


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')


# In[ ]:


response = table.scan()
data = response['Items']

while 'LastEvaluatedKey' in response:
    response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
    data.extend(response['Items'])


# In[ ]:


pos = []
count = 1
for val in data:
    pos.append({ "index" : {"_index": 'restaurants', "_id" : str(count)} })
    #pos.append({ "index" : {"_index": val['cuisine'], "_id" : str(count)} }) #for index = cuisine name
    temp = {}
    temp['cuisine'] = val['cuisine']
    temp['id'] = val['business_id']
    pos.append(temp)
    count += 1


# In[ ]:


json_file = open('yelpData.json', 'w+')
for val in pos:
    json_file.write(json.dumps(val))
    json_file.write('\n')
json_file.close()


# In[ ]:


##use curl to upload data to ElasticSearch
#curl -H 'Content-Type: application/x-ndjson' -XPOST 'https://search-dinning-chatbot-l3yczexyo4ubngidq7glbcbp4m.us-east-1.es.amazonaws.com/restaurants/_bulk?pretty' --data-binary '@yelp_data.json'

