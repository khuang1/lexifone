'''
*****************************************************************************
*  COPYRIGHT (c) 2015, Vonage.
*  All rights reserved. Property of Vonage Holdings, Corp.
*  Restricted rights to use, duplicate or disclose this code are granted
*  througt contract.
*
*  File Name: getLexifonePass.py
*
*  History Log:
*    Created: 10/20/2015
*    Author:  Kevin Huang
*    Version: 1.0.0.1
*****************************************************************************
'''
import boto3
import json
import random
import time
import datetime

print('Loading function')

def lambda_handler(event, context):
    '''Provide an event that contains the following keys:
      - operation: one of the operations in the operations dict below
      - tableName: required for operations that interact with DynamoDB
      - payload: a parameter to pass to the operation being performed
    '''
    #print("Received event: " + json.dumps(event, indent=3))

    operation = event['operation']
    clientVal = event['payload']

    dynamo = boto3.resource('dynamodb').Table('LexifonUsers')
    dynamo.load()

    operations = {
        'retPass':  lambda x: dynamo.update_item (
                                Key={'ID': clientVal['user']},
                                AttributeUpdates={
                                    'Status': {
                                        'Value': 'available',
                                        'Action': 'PUT'
                                    },
                                    'User' : {
                                        'Value': 'na',
                                        'Action': 'PUT'
                                    },
                                    'Updatetime': {
                                        'Value': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                                        'Action': 'PUT'
                                    }
                                },
                                Expected={ 'ID': {'Value': clientVal['user']}}
                            ),
        'getPass':  lambda x: dynamo.query(
                                IndexName='Status-index',
                                Select='SPECIFIC_ATTRIBUTES',
                                AttributesToGet=['ID', 'Password'],
                                KeyConditions={
                                    'Status': {
                                        'AttributeValueList': ['available'],
                                        'ComparisonOperator': 'EQ'
                                    }
                                },
                                Limit=100
                            ),
    }

    if operation in operations:
        response = operations[operation](event['payload'])
        if operation == 'getPass':
            totalCount = response['Count']
            if totalCount > 0:
                selitem = (response['Items'][random.randint(0, totalCount-1)])
                dynamo.update_item (
                    Key={'ID': selitem['ID']},
                    AttributeUpdates={
                        'Status': {
                            'Value': 'in-use',
                            'Action': 'PUT'
                        },
                        'User' : {
                            'Value': clientVal['user'],
                            'Action': 'PUT'
                        },
                        'Updatetime': {
                            'Value': datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S'),
                            'Action': 'PUT'
                        }
                    },
                    Expected={ 'ID': {'Value': selitem['ID']}}
                )
                return (selitem)
            else:
                raise ValueError('No pass is available in database "{}"'.format(operation))
        else:
            return (response)
    else:
        raise ValueError('Unrecognized operation "{}"'.format(operation))

