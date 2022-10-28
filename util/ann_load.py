import uuid
import time
import json
from datetime import datetime
# from dateutil import tz
# from dateutil import parser

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

# from gas import app, db
# from decorators import authenticated, is_premium
# from auth import get_profile, update_profile

def ann_load_test():

    for i in range(100):
        job_id = "9e6c1fcb-1dbb-4464-9cad-31124e2a51b1"
        user_id = "b8b3cc9b-de90-483f-b123-8792887c87ba"
        input_file = "test.vcf"
        bucket_name = "gas-inputs"
        key_input_file = "srirama/b8b3cc9b-de90-483f-b123-8792887c87ba/9e6c1fcb-1dbb-4464-9cad-31124e2a51b1~test.vcf"
        epoch = "1591636468"
        data = { 'job_id': job_id, 'user_id': user_id, 'input_file_name': input_file,   
        "s3_inputs_bucket": bucket_name, 
        "s3_key_input_file": key_input_file, 
        "submit_time": epoch,
        "job_status": "PENDING",
        }


        #Source - https://www.edureka.co/community/55307/python-code-to-publish-a-message-to-aws-sns

        #Publishing a notification with data being persisted in DynamoDB to SNS topic to request annotator job
        sns = boto3.client('sns', region_name = 'us-east-1')
        try:
            response  = sns.publish(

                TopicArn = "arn:aws:sns:us-east-1:127134666975:srirama_job_requests", 
                #Source - https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
                Message = json.dumps({'default': json.dumps(data)}),
                MessageStructure='json',
            )
        except (ClientError, ParamValidationError) as e:
            print(e.response)
            raise


if __name__ == "__main__":
    #Run an infinite loop
    while True:
        ann_load_test()