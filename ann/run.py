# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import sys
import time
import boto3
#import boto
import driver
from datetime import datetime
import subprocess
from botocore.exceptions import ClientError
from boto3.dynamodb.conditions import Key, Attr
import os
import json
from botocore.exceptions import ParamValidationError
# Get util configuration
from configparser import SafeConfigParser
config = SafeConfigParser(os.environ)
config.read(os.path.join(os.path.abspath(os.path.dirname(__file__)), 'ann_config.ini'))

"""A rudimentary timer for coarse-grained profiling
"""
class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
      print(f"Approximate runtime: {self.secs:.2f} seconds")

if __name__ == '__main__':

  if len(sys.argv) > 1:
    
    with Timer():
      driver.run(sys.argv[1], 'vcf')

    s3 = boto3.resource('s3')
    BUCKET = config['aws']['ResultsBucket']

    try:
      prec_file = sys.argv[1].split('~')[0]
      act_file = sys.argv[1].split('~')[1]
      act_file2 = act_file.split('.')[0]
      annot_file = prec_file+"~"+act_file2 +".annot.vcf"
      annot_filename = act_file2+".annot.vcf"
      log_file = prec_file+"~"+act_file2+".vcf.count.log"
      log_filename = act_file2+".vcf.count.log"
    except:
      print("The given argument did not have the file name.")
      raise

    #Update job status, completion time, the results bucket, results files to DynamoDB 
    dynamodb = boto3.resource('dynamodb', region_name=config['aws']['AwsRegionName'])
    ann_table = dynamodb.Table(config['aws']['DynamoTable']) 

    #Source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html
    try:
      response = ann_table.query(
        KeyConditionExpression=Key('job_id').eq(prec_file)
      )
    except ClientError as e:
      print(e.response)
      raise


    for i in response['Items']:
      user_id = i['user_id'] 

    #After completing the annotation job, upload the log and annotation file to the gas-results s3 bucket
    try:
      key = config['aws']['Key'] + "/" + user_id + "/"
      s3.Bucket(BUCKET).upload_file(annot_file,key+annot_file)
      s3.Bucket(BUCKET).upload_file(log_file,key+log_file)
    except ClientError as e:
      print(e.response)
      raise

    #Remove all the corresponding files in the local folder
    try:
      annotation_path = "/home/ubuntu/ann/"+ annot_file
      log_path = "/home/ubuntu/ann/"+ log_file
      sys_path = "/home/ubuntu/ann/" + sys.argv[1]
      os.remove(annot_file)
      os.remove(log_file)
      os.remove(sys.argv[1])
    except OSError:
      print("No such files in the system")
      raise
      
    
    #Get completion time for the job
    t = time.localtime()
    now = datetime.now()
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    pattern = '%d/%m/%Y %H:%M:%S'
    #Source - https://stackoverflow.com/questions/7241170/how-to-convert-current-date-to-epoch-timestamp
    epoch = int(time.mktime(time.strptime(dt_string, pattern)))


    #Source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html
    try:
      response = ann_table.query(
        KeyConditionExpression=Key('job_id').eq(prec_file)
      )
    except ClientError as e:
      print(e.response)
      raise

    for i in response['Items']:
      user_id = i['user_id'] 

    #Set the relevant parameters in DynamoDB after job completion
    try:
      #source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
      ann_table.update_item(
          Key = {'job_id': prec_file},
          UpdateExpression = 'set job_status = :j, s3_results_bucket = :rb, s3_key_results_file = :rf, s3_key_log_file = :lf, complete_time = :ct',
          ExpressionAttributeValues = {
              ':j' : "COMPLETED",
              ':rb' : config['aws']['ResultsBucket'],
              ':rf' : config['aws']['Key'] + "/" + user_id+"/"+annot_file,
              ':lf' : config['aws']['Key'] + "/" + user_id+"/"+log_file,
              ':ct' : epoch,
          },
          ReturnValues = "UPDATED_NEW"

      )
    except ClientError as e:
        #source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.03.html
        if e.response['Error']['Code'] == "ConditionalCheckFailedException":
            print(e.response['Error']['Message'])
        else:
            raise
    
    #Open file to get email
    try:
      f2 = open(prec_file, "r")
      email = f2.read()
    except OSError:
      print("No such files in the system")
      raise

    #Remove the email file from the system
    #Source - https://www.geeksforgeeks.org/python-os-remove-method/#:~:text=os.,be%20raised%20by%20the%20method.
    try:
        os.remove(prec_file)
    except OSError:
        print("No such files in the system")
        raise

    data = { 'job_id': prec_file, 'user_id': user_id, 
    # "s3_input_bucket": config['aws']['InputsBucket'],   
    "s3_results_bucket": config['aws']['ResultsBucket'], 
    "s3_key_results_file" : config['aws']['Key'] + "/" + user_id+"/"+annot_file,
    "s3_key_log_file" : config['aws']['Key'] + "/" + user_id+"/"+log_file,
    "complete_time": epoch,
    "email": email,
    "job_status": "COMPLETED"
    }
    #Send a notification to results arn (notify.py), so a confirmation email can be sent
    sns = boto3.client('sns', region_name = config['aws']['AwsRegionName'])
    try:
        response  = sns.publish(

            TopicArn = config['aws']['ResultsArn'],
            #Source - https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
            Message = json.dumps({'default': json.dumps(data)}),
            MessageStructure='json',
        )
    except (ClientError, ParamValidationError) as e:
        print(e.response)
        raise

  else:
    print("A valid .vcf file must be provided as input to this program.")

### EOF
