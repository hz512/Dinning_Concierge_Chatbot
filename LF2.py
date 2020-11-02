from boto3.dynamodb.conditions import Key
from botocore.vendored import requests
import time
import random
import boto3
import json

sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table('yelp-restaurants') 
sns = boto3.client("sns")
SQS_URL = "https://sqs.us-east-1.amazonaws.com/866265759394/dinningChatbot"
ES_SQL = "https://search-dinning-chatbot-l3yczexyo4ubngidq7glbcbp4m.us-east-1.es.amazonaws.com/"

def getMsg(sqs):
    msg = sqs.receive_message(QueueUrl=SQS_URL, AttributeNames=['All'],
        MaxNumberOfMessages=1, MessageAttributeNames=['All'],
        VisibilityTimeout=1, WaitTimeSeconds=0)
    return msg


def delMsg(sqs, msg):
    sqs.delete_message(QueueUrl=SQS_URL, ReceiptHandle=msg['Messages'][0]['ReceiptHandle'])


def getFromES(cuisine):
    url = ES_SQL + cuisine + "/_search?size=1000"
    response = requests.get(url).json()['hits']
    ind = random.randint(0, response['total']['value'] - 1)
    business_id = response['hits'][ind]['_source']['id']
    return business_id
    
    
def sendSNS(business_id, phone_number, cuisine):
    response = table.query(KeyConditionExpression=Key('business_id').eq(business_id))['Items'][0]
    name, address, no_reviews, rating = \
            response['name'], response['address'], response['no_reviews'], response['rating']
    message = "Bonjour, this is your personal concierge. I found a great {} restaurant that matches your preference. The restaurant is {}, located at {}. This place has {} reviews and an average rating of {} on Yelp. I believe you will have a great time there.".format(cuisine, name, address, no_reviews, rating)
    # print(message)
    sns.publish(PhoneNumber= "1"+phone_number, Message=message)


def lambda_handler(event, context):
    attempt = 0
    while True:
        msg = getMsg(sqs)
        time.sleep(2)
        if 'Messages' in msg:
            cuisine = json.loads(msg['Messages'][0]['Body'])['cuisine']
            phone_number = json.loads(msg['Messages'][0]['Body'])['phone_number']
            business_id = getFromES(cuisine)
            sendSNS(business_id, phone_number, cuisine)
            print("Completed!")
            delMsg(sqs, msg)
            break
        attempt += 1
        if attempt > 20:
            break
    return {
        'statusCode': 200,
        'body': json.dumps('Completed!')
    }
