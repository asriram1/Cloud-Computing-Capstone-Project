# archive.py
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
from boto3.dynamodb.conditions import Key, Attr
# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('archive_config.ini')

# Add utility code here
def archive_data():

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
            job_id = my_message['job_id']
            


        except KeyError:
            #If there is no message then break out of loop
            break



        
        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        ann_table = dynamodb.Table(config['aws']['DynamoTable']) 

        #Query the table using the job_id to get all files associated with the job
        #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query

        try:
            response = ann_table.query(
                KeyConditionExpression= Key('job_id').eq(job_id)
            )
        except ClientError as e:
            print(e.response)
            raise
        #Get Job Status
        for i in response['Items']:
            job_status = i['job_status']

        #If job status is not yet completed wait
        while job_status != "COMPLETED":
            try:
                response = ann_table.query(
                    KeyConditionExpression= Key('job_id').eq(job_id)
                )
            except ClientError as e:
                print(e.response)
                raise

            for i in response['Items']:
                job_status = i['job_status'] 
            
        #Once job status is completed wait another 5 minutes from complete time
        try:
            response = ann_table.query(
                KeyConditionExpression=Key('job_id').eq(job_id)
            )
        except ClientError as e:
            print(e.response)
            raise

        for i in response['Items']:
            complete_time = i['complete_time']
            s3_key_results_file = i['s3_key_results_file']



        
        datetime_object = time.strftime('%Y-%m-%d %H:%M', time.localtime(complete_time))
        datetime_object = parser.parse(datetime_object)
        a = datetime.now()
        difference = a - datetime_object
        seconds_in_day = 24 * 60 * 60
        #source - https://stackoverflow.com/questions/1345827/how-do-i-find-the-time-difference-between-two-datetime-objects-in-python
        x = divmod(difference.days * seconds_in_day + difference.seconds, 60)
        while x[0] <= 5:
            a = datetime.now()
            difference = a - datetime_object
            seconds_in_day = 24 * 60 * 60
            x = divmod(difference.days * seconds_in_day + difference.seconds, 60)

        files = []
        if x[0] > 5:
            bucket = s3.Bucket(config['aws']['ResultsBucket'])
            results_file_dir = "/".join(s3_key_results_file.split("/",2)[:2])
            results_file = "/".join(s3_key_results_file.split("/",2)[2:])
            files.append(s3_key_results_file)
            #Try download given file input from bucket
            #Source - https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
            for file in files:
                try:
                    bucket.download_file(file, results_file)
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
                
                #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.upload_archive
                my_file = open(results_file,"r")
                my_file = my_file.read()
                response = client.upload_archive(
                    vaultName=config['aws']['VaultName'],
                    body = my_file
                )
                
                #Delete object from S3
                #Source - https://stackoverflow.com/questions/3140779/how-to-delete-files-from-amazon-s3-bucket
                try:
                    s3 = boto3.resource('s3')
                    s3.Object(config['aws']['ResultsBucket'], s3_key_results_file).delete()
                except ClientError as e:
                    print("Unable to delete object!")
                    print(e.response)
                    raise

                try:
                    os.remove(results_file)
                except OSError:
                    print("No such files in the system")
                    raise

            archive_id = response['archiveId']
            dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
            ann_table = dynamodb.Table(config['aws']['DynamoTable']) 

            try:
                #source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                ann_table.update_item(
                    Key = {'job_id': job_id},
                    UpdateExpression = 'set results_file_archive_id = :j',
                    ExpressionAttributeValues = {
                        ':j' : archive_id,
                    },
                    ReturnValues = "UPDATED_NEW"

                )
            except ClientError as e:
                #source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                if e.response['Error']['Code'] == "ConditionalCheckFailedException":
                    print(e.response['Error']['Message'])
                else:
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
        archive_data()



### EOF