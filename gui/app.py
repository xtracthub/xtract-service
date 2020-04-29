
from flask import Flask, redirect, jsonify, url_for,render_template, request,session, flash, send_from_directory
from datetime import timedelta
import os
from werkzeug.utils import secure_filename
import boto3

# upload_folder = "/tmp/"
# ALLOWED_EXTENSIONS = {'json'}
# if not os.path.exists(upload_folder):
#     os.makedirs(upload_folder)
#
#
# class Config:
#     DEBUG = True
#     BOTO3_SERVICES = ['S3', 's3']
#     UPLOAD_FOLDER = upload_folder


app = Flask(__name__)
# app.config.from_object(Config)
app.secret_key = "foobar"
app.permanent_session_lifetime = timedelta(minutes=5) # defines how long the session will last

# Get the service resource
# sqs = boto3.resource('sqs')

# Get queue from AWS. Returns a SQS.Queue instance
# queue = sqs.get_queue_by_name(QueueName='test')

# You can now access identifiers and attributes
# print(queue.url)
# print(queue.attributes.get('DelaySeconds'))


@app.route('/sendMessage', methods=['GET','POST'])
def sendMessage():
    if request.method == 'POST':
        # Create a new message
        message = request.form['message']
        print(message)
        if 'message' == '':
            flash('No input made.')
        else:
            # response = queue.send_message(MessageBody=message)
            # print(response.get('MessageId'))
            # print(response.get('MD5OfMessageBody'))
            print("ELSE.")
    return '''
    <!doctype html>
    <title>Send Message</title>
    <h1>Send Message</h1>
    <form method=post enctype=multipart/form-data>
      <input type=text name=message>
      <input type=submit value=Upload>
    </form> '''