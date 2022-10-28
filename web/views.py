# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime
from dateutil import tz
from dateutil import parser

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError
from botocore.exceptions import ParamValidationError

from flask import (abort, flash, redirect, render_template, 
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3', 
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  #Connect to DynamoDB table
  dynamodb = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  user_id = session['primary_identity']

  #Parsing for the bucket, key, and jobID
  rule = request.url
  rule = str(rule)

  try:
      bucket_name_temp = rule.split("bucket=")[1]
      bucket_name = bucket_name_temp.partition('&')[0]
      key_temp = bucket_name_temp.split('&key=')[1]
      key = key_temp.partition("%")[0]
      job_id_temp = key_temp.partition("%")[2]
      job_id_temp = job_id_temp.partition("%")[2]
      job_id_temp = job_id_temp.partition("2F")[2]
      job_id_temp = job_id_temp.partition("&")[0]
      job_id = job_id_temp.partition('~')[0]
      input_file = job_id_temp.partition('~')[2]
      key_input_file = key + "/"+user_id+"/"+job_id+"~"+input_file

  except:
      print("The specified bucket or key variables could not be parsed in the url.")
      raise

  #Getting current time in epoch
  t = time.localtime()
  now = datetime.now()
  dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
  pattern = '%d/%m/%Y %H:%M:%S'
  #https://stackoverflow.com/questions/7241170/how-to-convert-current-date-to-epoch-timestamp
  epoch = int(time.mktime(time.strptime(dt_string, pattern)))
  
  email = session.get('email')

  #Data to be persisted in DynamoDB
  data = { 'job_id': job_id, 'user_id': user_id, 'input_file_name': input_file,   
  "s3_inputs_bucket": bucket_name, 
  "s3_key_input_file": key_input_file, 
  "submit_time": epoch,
  "email": email,
  "job_status": "PENDING",
  }

  #Try to put data into the Database
  #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.put_item
  try:
      ann_table.put_item(Item = data)
  except ClientError as e:
      print(e.response)
      raise

  #Source - https://www.edureka.co/community/55307/python-code-to-publish-a-message-to-aws-sns

  #Publishing a notification with data being persisted in DynamoDB to SNS topic to request annotator job
  sns = boto3.client('sns', region_name = app.config['AWS_REGION_NAME'])
  try:
      response  = sns.publish(

          TopicArn = app.config['AWS_SNS_JOB_REQUEST_TOPIC'], 
          #Source - https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
          Message = json.dumps({'default': json.dumps(data)}),
          MessageStructure='json',
      )
  except (ClientError, ParamValidationError) as e:
      print(e.response)
      raise
  
  #If the user is a free user, then send a notification to archive the file
  if session['role'] == "free_user":
    try:
      response  = sns.publish(

          TopicArn = app.config['AWS_SNS_ARCHIVE_TOPIC'], 
          #Source - https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
          Message = json.dumps({'default': json.dumps(data)}),
          MessageStructure='json',
      )
    except (ClientError, ParamValidationError) as e:
        print(e.response)
        raise


  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():

  #Connect to DynamoDB annotations table
  dynamodb = boto3.resource('dynamodb', region_name= app.config['AWS_REGION_NAME'])
  
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 
  user_id = session['primary_identity']
  try:
    #Source - https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.Python.04.html
    response = ann_table.query(
      IndexName = "user_id_index",
      KeyConditionExpression=Key('user_id').eq(user_id),
    )
  except ClientError as e:
    print(e.response)
    raise

  response_list = []
  to_zone = tz.tzlocal()
  


  for i in response['Items']:
      temp_dict = {}
      temp_dict['job_id'] = i['job_id']
      epoch_time = i['submit_time']
      #Convert submit time to given datetime format
      #Source - https://stackoverflow.com/questions/12400256/converting-epoch-time-into-the-datetime
      temp_dict['submit_time']= time.strftime('%Y-%m-%d %H:%M', time.localtime(epoch_time))
      temp_dict['input_file_name'] = i['input_file_name']
      temp_dict['job_status'] = i['job_status']
      response_list.append(temp_dict)

  # Get list of annotations to display
  
  return render_template('annotations.html', annotations=response_list)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):

  sns = boto3.client('sns', region_name = app.config['AWS_REGION_NAME'])

  dynamodb = boto3.resource('dynamodb', region_name= app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 
  user_id = session['primary_identity']

  try:
    #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    response = ann_table.query(
      KeyConditionExpression=Key('job_id').eq(id)
    )
  except ClientError as e:
    print(e.response)
    raise

  response_list = []
  for i in response['Items']:
    #If the given job is not currently running, then check for output files
    if i['job_status'] != "RUNNING":
      s3_key_results_file = i['s3_key_results_file'] 

      s3 = boto3.client('s3', 
        region_name=app.config['AWS_REGION_NAME'],
        config=Config(signature_version='s3v4'))
      

      bucket_name = app.config['AWS_S3_RESULTS_BUCKET']
      key_name = s3_key_results_file

      #If premium user, and restore for file in progress, then show Message
      s3 = boto3.resource('s3')
      #Source - https://stackoverflow.com/questions/15085864/how-to-upload-a-file-to-directory-in-s3-bucket-using-boto
      try:
          s3.Object(bucket_name, key_name).load()
      except ClientError as e:
          if e.response['Error']['Code'] == "404":
              # The object does not exist.
              restore_message = True
          else:
              # Something else has gone wrong.
              raise
      else:
          # The object does exist.
          restore_message = False
      
      #If the object does exist then get presigned url for download
      #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-presigned-urls.html
      if restore_message == False:
        try:
          s3 = boto3.client('s3', 
          region_name=app.config['AWS_REGION_NAME'],
          config=Config(signature_version='s3v4'))
          presigned_url = s3.generate_presigned_url(
            'get_object',
            Params = { 'Bucket': bucket_name, 
            'Key': key_name},
            ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
        except ClientError as e:
          app.logger.error(f"Unable to generate presigned URL for upload: {e}")
          return abort(500)
      else:
        presigned_url = ""

      #Parse the response for other parameters such as submit_time, complete_time etc.
      temp_dict = {}
      temp_dict['job_id'] = i['job_id']
      epoch_submit_time = i['submit_time']
      temp_dict['submit_time']= time.strftime('%Y-%m-%d %H:%M', time.localtime(epoch_submit_time))
      temp_dict['input_file_name'] = i['input_file_name']
      temp_dict['job_status'] = i['job_status']
      epoch_complete_time = i['complete_time']
      temp_dict['complete_time']= time.strftime('%Y-%m-%d %H:%M', time.localtime(epoch_complete_time))
      temp_dict['s3_key_results_file'] = i['s3_key_results_file']
      datetime_object = parser.parse(temp_dict['complete_time'])
      #If the user is a free user check if 5 minutes has passed since complete time to determine access expiry
      if (session.get('role') == "free_user"):
        a = datetime.now()
        difference = a - datetime_object
        seconds_in_day = 24 * 60 * 60
        #source - https://stackoverflow.com/questions/1345827/how-do-i-find-the-time-difference-between-two-datetime-objects-in-python
        x = divmod(difference.days * seconds_in_day + difference.seconds, 60)
        if x[0]>5:
          free_access_expired = True
        else:
          free_access_expired = False
      else:
        free_access_expired = False

      #If there is a restore message then display it
      if restore_message == True:
        temp_dict['restore_message'] = 'The file is currently restoring!'
      else:
        temp_dict['result_file_url'] = presigned_url

      response_list.append(temp_dict) 
    else:
      #If the job is still running get the relevant parameters.
      free_access_expired = False
      temp_dict = {}
      temp_dict['job_id'] = i['job_id']
      epoch_submit_time = i['submit_time']
      temp_dict['submit_time']= time.strftime('%Y-%m-%d %H:%M', time.localtime(epoch_submit_time))
      temp_dict['input_file_name'] = i['input_file_name']
      temp_dict['job_status'] = i['job_status']
      response_list.append(temp_dict)



      

  return render_template('annotation_details.html', annotation=response_list[0], free_access_expired = free_access_expired)



"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  
  dynamodb = boto3.resource('dynamodb', region_name= app.config['AWS_REGION_NAME'])
  ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 

  try:
    #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    response = ann_table.query(
      KeyConditionExpression=Key('job_id').eq(id)
    )
  except ClientError as e:
    print(e.response)
    raise
  #Query using the job id and get the log file name
  for i in response['Items']:
    s3_key_log_file = i['s3_key_log_file'] 

  bucket= app.config['AWS_S3_RESULTS_BUCKET']
  key = s3_key_log_file

  #Source - https://stackoverflow.com/questions/31976273/open-s3-object-as-a-string-with-boto3
  s3 = boto3.resource('s3')
  obj = s3.Object(bucket, key)
  #Read the log file
  response = obj.get()['Body'].read().decode('utf-8') 


  return render_template('view_log.html', job_id = id, log_file_contents=response)


"""Subscription management handler
"""
import stripe

@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Process the subscription request
    token = str(request.form['stripe_token']).strip()

    # Create a customer on Stripe
    stripe.api_key = app.config['STRIPE_SECRET_KEY']
    try:
      customer = stripe.Customer.create(
        card = token,
        plan = "premium_plan",
        email = session.get('email'),
        description = session.get('name')
      )
    except Exception as e:
      app.logger.error(f"Failed to create customer billing record: {e}")
      return abort(500)

    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    user_id = session['primary_identity']

    dynamodb = boto3.resource('dynamodb', region_name= app.config['AWS_REGION_NAME'])
    ann_table = dynamodb.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE']) 
    #Query the table using the user_id to get all files associated with the user
    #Source - https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
    try:
      response = ann_table.query(
        IndexName = "user_id_index",
        KeyConditionExpression=Key('user_id').eq(user_id),
      )
    except ClientError as e:
      print(e.response)
      raise
    
    for i in response['Items']:
      if 'results_file_archive_id' in i:
        #If there is any files for the user to be restored, restore them
        if i['results_file_archive_id'] != None and i['results_file_archive_id'] != "restored":
          results_file_archive_id = i['results_file_archive_id']
          s3_key_results_file = i['s3_key_results_file']
          s3_results_bucket = i['s3_results_bucket']

          data = {}
          data['results_file_archive_id'] = results_file_archive_id
          data['s3_key_results_file'] = s3_key_results_file
          data['s3_results_bucket'] = s3_results_bucket

          #Publish to restore SNS topic
          sns = boto3.client('sns', region_name = app.config['AWS_REGION_NAME'])
          try:
            response  = sns.publish(

                TopicArn = app.config['AWS_SNS_RESTORE_TOPIC'], 
                #Source - https://stackoverflow.com/questions/34029251/aws-publish-sns-message-for-lambda-function-via-boto3-python2
                Message = json.dumps({'default': json.dumps(data)}),
                MessageStructure='json',
            )
          except (ClientError, ParamValidationError) as e:
              print(e.response)
              raise



    # Display confirmation page
    return render_template('subscribe_confirm.html', 
      stripe_customer_id=str(customer['id']))


"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF