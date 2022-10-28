# thaw.py
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
from botocore.exceptions import ClientError
#from Glacier.Client.exceptions import ResourceNotFoundException

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('thaw_config.ini')

# Add utility code here

def thaw_data():

    client = boto3.client('glacier', region_name = config['aws']['AwsRegionName'])
    sqs = boto3.client('sqs', region_name = config['aws']['AwsRegionName'])
    queue_url = config['aws']['QueueURL']
    session = Session()
    s3 = session.resource('s3')
    permanent_dict = {}
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

            #When the restoration job completes  
            try:
                archiveId = my_message['ArchiveId']
                jobId = my_message['JobId']
                statusCode = my_message['StatusCode']
                s3_key_results_file = 0
            except:
                #When the restoration job is started
                s3_key_results_file = my_message['s3_key_results_file']
                jobId = my_message['jobId']
                #Write the results file name to a file called <jobId>
                f = open(jobId, "w")
                f.write(s3_key_results_file)
                f.close()
                statusCode = "Unknown"

        except KeyError:
            #If there is no message then break out of loop
            break

        
        if s3_key_results_file ==0: 
            #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.get_job_output
            #Get the output from the archive file
            try:
                response = client.get_job_output(vaultName = config['aws']['VaultName'], jobId = jobId)
            except ClientError as e:
                print("Input a valid jobID!")
                raise ClientError
            
            #Write output to the results filename
            data = response['body']
            try:
                f2 = open(jobId, "r")
                s3_key_results_file = f2.read()
                annotation = data.read().decode("utf-8")
                user_id_temp = s3_key_results_file.split(config['aws']['Key'] + "/")[1]
                user_id = user_id_temp.split("/")[0]
                filename = user_id_temp.split("/")[1]
                annotation_id = filename.split("~")[0]
            except OSError:
                print("No such files in the system")
                raise

            f = open(filename, 'w')
            f.write(annotation)
            f.close()

            #Source - https://stackoverflow.com/questions/37017244/uploading-a-file-to-a-s3-bucket-with-a-prefix-using-boto3
            #Upload given file back to S3
            BUCKET = config['aws']['ResultsBucket']
            s3 = boto3.resource('s3')
            try:
                key = config['aws']['Key'] + "/" + user_id + "/"
                s3.meta.client.upload_file(filename, BUCKET , key+filename)
            except ClientError as e:
                print(e.response)
                raise
        

        if statusCode == 'Succeeded':
            #Delete the archive from S3
            #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/glacier.html#Glacier.Client.delete_archive
            try:
                delete_response = client.delete_archive(vaultName = config['aws']['VaultName'], archiveId = archiveId)
            except ClientError as e:
                print("Could not find archive!")
                raise ClientError
            
            #Remove the files created from the local system
            #Source - https://www.geeksforgeeks.org/python-os-remove-method/#:~:text=os.,be%20raised%20by%20the%20method.
            try:
                os.remove(filename)
                os.remove(jobId)
            except OSError:
                print("No such files in the system")
                raise
            

            dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
            ann_table = dynamodb.Table(config['aws']['DynamoTable']) 

            #Update DynamoDB to remove archiveID from job and mention restored
            try:
                #source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
                ann_table.update_item(
                    Key = {'job_id': annotation_id},
                    UpdateExpression = 'set results_file_archive_id = :j',
                    ExpressionAttributeValues = {
                        ':j' : "restored",
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
        thaw_data()



### EOF