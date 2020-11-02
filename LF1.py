#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import boto3
import datetime
import dateutil.parser
import json
import logging
import math
import os
import time
import sys
from botocore.vendored import requests

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return dispatch(event)
    
    
def dispatch(intent_request):
    logger.debug(
        'dispatch userId={}, intentName={}'.format(intent_request['userId'], intent_request['currentIntent']['name']))
    intent_name = intent_request['currentIntent']['name']
    if intent_name == 'Greeting':
        return greeting(intent_request)
    elif intent_name == 'DiningSuggestions':
        return diningSuggestions(intent_request)
    elif intent_name == 'ThankYou':
        return thankYou(intent_request)
    raise Exception('Intent with name ' + intent_name + ' not supported')


def greeting(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': 'Hi, I am your personal concierge. How may I help you?'
            }
        }
    }


def thankYou(intent_request):
    return {
        'dialogAction': {
            "type": "ElicitIntent",
            'message': {
                'contentType': 'PlainText',
                'content': "It is my pleasure helping you."
            }
        }
    }


def varificationResult(is_valid, violated_slot, message):
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': {'contentType': 'PlainText', 'content': message}
    }
 
    
def validDate(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False
        
        
def parseInt(n):
    try:
        return int(n)
    except ValueError:
        return float('nan')
        
    
def diningSuggestion(location, cuisine, no_people, date, time, phone_number):
    locations = {'lower east side', 'upper east side', 'upper west side', 
    'washington heights', 'central harlem', 'soho', 'chelsea', 
    'east harlem', 'gramercy park', 'lower manhattan'}
    cuisines = {'pizza', 'chinese', 'mexican', 'burgers', 'breakfast', 'thai',
        'italian', 'steakhouse', 'korean', 'seafood', 'japanese', 'vietnamese',
        'sandwiches', 'vegetarian', 'sushi bars', 'american'}
    if location is not None and location.lower() not in locations:
        if location.lower() == "manhattan":
            return varificationResult(False, 'location',
                   'Please specify a neighborhood in Manhattan.')
        else:
            return varificationResult(False, 'location',
               'Only following neighborhoods are currently supported: ' + str(list(locations)))
    if cuisine is not None and cuisine.lower() not in cuisines:
        return varificationResult(False, 'cuisine',
                   'I do not have any good options for {} food.  \
                   Please try a different cuisine.'.format(cuisine))
    if no_people is not None and int(no_people) < 0:
        return varificationResult(False, 'no_people',
                   'The number of people is invalid. Please try again.')
    if date is not None:
        if not validDate(date):
            return varificationResult(False, 'dinning_date',
            'The date you entered is invalid. Please try again.')
        elif datetime.datetime.strptime(date, '%Y-%m-%d').date() < datetime.date.today():
            return varificationResult(False, 'dinning_date', 
            'The date you entered is in the past. Please try again.')
    if time is not None:
        if len(time) != 5:
            return varificationResult(False, 'dinning_time', 
        'The time you entered is in invalid format.')
        hour, minute = time.split(':')
        hour,  minute = parseInt(hour), parseInt(minute)
        if math.isnan(hour) or math.isnan(minute):
            return varificationResult(False, 'dinning_time', 
            'The date you entered is invalid. Please try again.')
    if phone_number is not None:
        pn = phone_number.replace('-', '')
        pn = phone_number.replace('(', '')
        pn = phone_number.replace(')', '')
        if len(pn) != 10 or not pn.isnumeric():
            return varificationResult(False, 'phone_number',
            "Please enter a 10-digit phone number like 123-456-7890.")
            
    return varificationResult(True, None, None)
    

def diningSuggestions(intent_request):
    slots = intent_request['currentIntent']['slots']
    location = slots["location"]
    cuisine = slots["cuisine"]
    no_people = slots["no_people"]
    date = slots["dinning_date"]
    time = slots["dinning_time"]
    phone_number = slots["phone_number"] 
    
    if intent_request['invocationSource'] == 'DialogCodeHook':
        validation_result = diningSuggestion(location, 
                    cuisine, no_people, date, time, phone_number)
        if not validation_result['isValid']:
            slots[validation_result['violatedSlot']] = None
            return {
                'sessionAttributes': intent_request['sessionAttributes'],
                'dialogAction': {
                    'type': 'ElicitSlot',
                    'intentName': intent_request['currentIntent']['name'],
                    'slots': slots,
                    'slotToElicit': validation_result['violatedSlot'],
                    'message': validation_result['message']
                }
            }
        sa = intent_request['sessionAttributes'] if intent_request['sessionAttributes'] is not None else {}
        return {
            'sessionAttributes': sa,
            'dialogAction': {
                'type': 'Delegate',
                'slots': intent_request['currentIntent']['slots']
            }
        }
    
    sqs = boto3.resource('sqs')
    queue = sqs.get_queue_by_name(QueueName='dinningChatbot')
    msg = {'cuisine': str(cuisine), 'phone_number': str(phone_number)}
    response = queue.send_message(MessageBody=json.dumps(msg))
    return {
        'sessionAttributes': intent_request['sessionAttributes'],
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': 'Fulfilled',
            'message': {'contentType': 'PlainText',
              'content': 'Thanks for your info! You will recieve your suggestion shortly.'
            }
        }
    }
    

