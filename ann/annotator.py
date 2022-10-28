from flask import Flask, request, jsonify, render_template
import os.path
import os
import requests
import uuid
import sys
from subprocess import Popen, PIPE, CalledProcessError
import boto3
from botocore.client import Config
import botocore
from botocore.exceptions import ClientError
from boto.s3.connection import S3Connection
from boto3.session import Session
import json

# Get util configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

def access_files():
    sqs = boto3.client('sqs', region_name = config['aws']['AwsRegionName'])
    queue_url = config['aws']['QueueURL']


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
            user_id = my_message['user_id']    
            input_file_name = my_message['input_file_name']  
            s3_inputs_bucket = my_message['s3_inputs_bucket']
            s3_key_input_file = my_message['s3_key_input_file']
            submit_time = my_message['submit_time']
            job_status = my_message['job_status']
            email = my_message['email']

        except KeyError:
            #If there is no message then break out of loop
            break
        
        #Write the user email to jobID file so this can be sent to notify.py (sent in run.py)
        my_path = "/home/ubuntu/ann/"+ job_id
        f = open(my_path, "w")
        f.write(email)
        f.close()


        #Open S3 gas-inputs bucket
        session = Session()

        s3 = session.resource('s3')

        try:
            bucket = s3.Bucket(config['aws']['BucketName'])
            #List all objects in srirama subfolder
            prefix = config['aws']['Key']+"/"
            objs = list(bucket.objects.filter(Prefix=prefix))
        except botocore.exceptions.ClientError as e:
            print(e.response)
            raise
        
        #Changed to include user_id
        input_file = config['aws']['Key']+ '/'+ user_id+"/"+ job_id + "~" + input_file_name
        files = []
        files.append(input_file)

        #Try download given file input from bucket
        #Source - https://boto3.amazonaws.com/v1/documentation/api/1.9.42/guide/s3-example-download-file.html
        for file in files:
            try:
                file_address = "/home/ubuntu/ann/"+os.path.basename(file)
                bucket.download_file(file, file_address)
            except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == "404":
                    print("The object does not exist.")
                else:
                    raise
        
        #Run the annotator on the file
        for file in files:
            #Changed to include user_id
            myfile = file.split('/')[2]
            command = 'python /home/ubuntu/ann/run.py '+myfile
            try:
                p = Popen(command, shell = True)
            except CalledProcessError as e:
                print(e.output)
                raise

        #All data parsed from message
        data = { 'job_id': job_id, 'user_id': user_id, 'input_file_name': input_file_name,   
        "s3_inputs_bucket": s3_inputs_bucket, 
        "s3_key_input_file": s3_key_input_file, 
        "submit_time": submit_time,
        "job_status": "RUNNING",
        }

        #Update the given item in the DynamoDB to Running
        dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
        ann_table = dynamodb.Table(config['aws']['DynamoTable'])

        #Only update to running, if the current job status is pending and not already completed
        try:
            #source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
            ann_table.update_item(
                Key = {'job_id': job_id},
                UpdateExpression = 'set job_status = :j',
                ConditionExpression = "job_status = :val",
                ExpressionAttributeValues = {
                    ':j' : "RUNNING",
                    ':val': "PENDING",
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
        access_files()


