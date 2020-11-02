import json
import boto3

def lambda_handler(event, context):
    client = boto3.client('lex-runtime')
    response = client.post_text(
        botName='dinningChatbot',
        botAlias='dinningChatbot',
        userId='haoran',
        sessionAttributes={},
        requestAttributes={},
        inputText = event['messages'][0]['unstructured']['text']
    )
    return { 
        'statusCode': 200,
        'headers': {'Access-Control-Allow-Origin': '*'}, 
        'messages': [ 
            {'type': "unstructured", 
            'unstructured': {'text': response['message']}  
            } 
        ]
    }