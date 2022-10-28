# restore.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import boto3
import json
from datetime import datetime
from dateutil import parser
from boto3.session import Session
from botocore.exceptions import ClientError
#from botocore.exceptions import InsufficientCapacityException
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('restore_config.ini')


# Add utility code here
def restore_data():

    client = boto3.client('glacier', region_name = config['aws']['AwsRegionName'])
    sqs = boto3.client('sqs', region_name = config['aws']['AwsRegionName'])
    queue_url = config['aws']['QueueURL']
    session = Session()
    s3 = session.resource('s3')

    #Source - https://alexwlchan.net/2018/01/downloading-sqs-queues/
    while True:

        #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/sqs-example-sending-receiving-msgs.html
        
        #Try to poll for a messages from the SQS (using long polling between polls)
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            WaitTimeSeconds = 5,
        )

        try:
            #Try to get a message from queue
            message = response['Messages'][0]
            receipt_handle = message['ReceiptHandle']

            body = message['Body']
            json_body = json.loads(body)
            json_body = json_body['Message']
            json_body = json.loads(json_body)
            my_message = json_body  
            archive_id = my_message['results_file_archive_id']
            s3_key_results_file = my_message['s3_key_results_file']
            s3_results_bucket = my_message['s3_results_bucket']


        except KeyError:
            #If there is no message then break out of loop
            break

        

        results_sub_bucket = "/".join(s3_key_results_file.split("/",2)[:2]) +"/"
        

        #Initiate Restore Job
        #Source- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.initiate_job
        
        try:
            restore_response = client.initiate_job(vaultName=config['aws']['VaultName'],jobParameters={'ArchiveId': archive_id , 'Type': "archive-retrieval", 'Tier': 'Expedited', 'SNSTopic': config['aws']['ThawArn'] } )
        except:
            try:   
                restore_response = client.initiate_job(vaultName=config['aws']['VaultName'],jobParameters={'ArchiveId': archive_id , 'Type': "archive-retrieval", 'Tier': 'Standard', 'SNSTopic': config['aws']['ThawArn'] } )
            except ClientError as e:
                print(e.response)
                raise

        

        restore_response['archiveId'] = archive_id
        jobId = restore_response['jobId']
        #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.describe_job
        #Describe job to get status
        try:
            thaw_response = client.describe_job(vaultName = 'ucmpcs', jobId = jobId)
        except ClientError as e:
            print(e.response)
            raise ClientError
        

        data = {}
        data['jobId'] = jobId
        data['s3_key_results_file'] = s3_key_results_file

        #Source - https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
        #Send a notification to Thaw SNS
        sns = boto3.client('sns', region_name = config['aws']['AwsRegionName'])
        try:
            response  = sns.publish(

                TopicArn = config['aws']['ThawArn'], 
                Message = json.dumps({'default': json.dumps(data)}),
                MessageStructure='json',
            )
        except (ClientError, ParamValidationError) as e:
            print(e.response)
            raise

        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        print('Received and deleted message: %s' % my_message)
        print("\n")    


if __name__ == "__main__":
    #Run an infinite loop
    while True:
        restore_data()


### EOF