#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import datetime
import boto3
import json


# In[ ]:


CLIENT_ID = "zMGoozUxHANWKoLMt-jm3w"
API_KEY = "zJxPDvciF7X_WhpgtC-zQdkkEmrpaBcRDKMbR5s-8TH_VLiwSa2jo-S-FighMWrQ_9-UTxbRcA4jO2Sn8F2M7KqFw-tI-eHpLXxZ6Gzz1cCyeWIqaBq-QoSCxiqWX3Yx"
URL = 'https://api.yelp.com/v3/businesses/search'
HEADER = {'Authorization': "Bearer " + API_KEY}

LOCATIONS = ['Lower East Side, Manhattan', 'Upper East Side, Manhattan', 'Upper West Side, Manhattan',
                'Washington Heights, Manhattan', 'Central Harlem, Manhattan', 'Soho, Manhattan',
                'Chelsea, Manhattan', 'East Harlem, Manhattan', 'Gramercy Park, Manhattan', 'Lower Manhattan, Manhattan']
CUISINES = ['pizza', 'chinese', 'mexican', 'burgers', 'breakfast', 'thai', 'italian', 'steakhouse', 
            'korean', 'seafood', 'japanese', 'vietnamese', 'sandwiches', 'vegetarian', 'sushi bars', 'american']

def check_empty(input):
    if len(str(input)) == 0:
        return 'N/A'
    else:
        return input

def search(location, cuisine, offset):
    params = {
        'location': location,
        'offset': offset,
        'limit': 50,
        'term': cuisine + " food",
        'sort_by': 'rating'
    }
    response = requests.get(URL, params=params, headers=HEADER)
    businessData = response.json()['businesses']
    return businessData


# #### Add Greenwich Village and Soho

# In[ ]:


dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('yelp-restaurants')


# In[ ]:


for cuisine in CUISINES:
    for location in LOCATIONS:
#         if cuisine in ['pizza', 'chinese', 'mexican', 'burgers']:
#             continue
        print("location:", location, "; cuisine:", cuisine)
        offset = 0
        while offset <= 150:
            businessData = search(location, cuisine, offset)
            for business in businessData:
                if len(business['location']['display_address']) == 0:
                    address = 'N/A'
                else:
                    address = ', '.join(business['location']['display_address'])
                item = {
                    'business_id': check_empty(business['id']),
                    'insertedAtTimestamp': str(datetime.datetime.now()),
                    'cuisine': cuisine,
                    'name':  check_empty(business['name']),
                    'address': address,
                    'latitude': check_empty(str(business['coordinates']['latitude'])),
                    'longitude': check_empty(str(business['coordinates']['longitude'])),
                    'no_reviews': check_empty(str(business['review_count'])),
                    'rating': check_empty(str(business['rating'])),
                    'zip_code': check_empty(business['location']['zip_code'])
                }
                table.put_item(Item=item)
            if len(businessData) < 50:
                offset += len(businessData)
                break
            offset += 50
        if offset == 200: offset = 150
        print("number of data scraped:", offset)
        print("----------------------------------------")

