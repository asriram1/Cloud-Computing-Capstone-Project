# notify.py
#
# NOTE: This file lives on the Utils instance
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
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

# Import utility helpers
sys.path.insert(1, os.path.realpath(os.path.pardir))
import helpers

# Get configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read('notify_config.ini')

# Add utility code here

def notify():
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
            s3_results_bucket = my_message['s3_results_bucket']
            s3_key_results_file = my_message['s3_key_results_file']
            s3_key_log_file = my_message['s3_key_log_file']
            complete_time = my_message['complete_time']
            job_status = my_message['job_status']
            email = my_message['email']

        except KeyError:
            #If there is no message then break out of loop
            break

        try:
            send_email_ses(recipients = email, subject = job_id, body = user_id )
        except ClientError as e:
            print(e.response)
            raise

        # Delete received message from queue
        sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=receipt_handle
        )

        print('Received and deleted message: %s' % my_message)
        print("\n")


"""Send email via Amazon SES
"""
def send_email_ses(recipients=None, 
  sender=None, subject=None, body=None):

  ses = boto3.client('ses', region_name=config['aws']['AwsRegionName'])

  try:
    response = ses.send_email(
      Destination = {
        'ToAddresses': (recipients if type(recipients) == "list" else [recipients])
      },
      Message={
        'Body': {'Text': {'Charset': "UTF-8", 'Data': body}},
        'Subject': {'Charset': "UTF-8", 'Data': subject},
      },
      Source=(sender or config['gas']['EmailDefaultSender']))
  except ClientError as e:
    raise ClientError

  return response
        

if __name__ == "__main__":
    #Run an infinite loop
    while True:
        notify()


### EOF