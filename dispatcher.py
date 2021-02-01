import numpy as np
import tweepy
import requests
import json
import boto3
from flask import Flask, request, redirect, url_for, flash, jsonify
import numpy as np
import pickle as p
import json
from config import *

kinesis_client = boto3.client('kinesis')

class StreamDispatcher():

    def __init__(self, api):
        self.streams = {}
        self.api = api
        self.filters = {}

    def add_stream(self, tag, **filters):
        print('Adding stream {} with filters {}'.format(tag, filters))
        myStreamListener = MyStreamListener(tag=tag)
        myStream = tweepy.Stream(auth = self.api.auth,
                                 listener=myStreamListener)
        self.streams[tag] = myStream
        self.streams[tag].filter(**filters, is_async=True)
        self.filters[tag] = filters

    def stop_stream(self, tag):
        print('Stopping stream {}'.format(tag))
        if not tag in self.streams:
            raise Exception('Unkown tag')
        self.streams[tag].disconnect()
        self.streams.pop(tag)

    def start_stream(self, tag):
        print('Starting stream {}'.format(tag))
        if not tag in self.streams:
            raise Exception('Unkown tag')
        self.streams[tag].filter(**self.filters[tag], is_async=True)

    def list_streams(self):
        return list(self.streams.keys())

class MyStreamListener(tweepy.StreamListener):

    def __init__(self, tag=None):
        self.tag = tag
        super().__init__()

    def on_data(self, data):
        tweet = json.loads(data)
        if "text" in tweet.keys():
            payload = [{'id': str(tweet['id']),
                       'tweet': str(tweet['text'].encode('utf8', 'replace')),
                       'ts': str(tweet['created_at']),
                       'tag': self.tag
            }]
            try:
                put_response = kinesis_client.put_record(
                                StreamName=stream_name,
                                Data=json.dumps(payload),
                                PartitionKey=str(tweet['user']['screen_name']))
            except (AttributeError, Exception) as e:
                print (e)
                pass
        return True

    def on_error(self, status_code):
        print('error',status_code)
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False

auth = tweepy.OAuthHandler(ACCESS_KEY, SECRET_KEY)
auth.set_access_token(ACCESS_TOKEN, SECRET_TOKEN)
api = tweepy.API(auth_handler=auth,)
dispatcher = StreamDispatcher(api)


app = Flask(__name__)

@app.route('/api/stop', methods=['POST'])
def stop_stream():
    data = request.get_json()
    try:
        if not 'tag' in data:
            raise Exception('tag has to be contained in request data')
        dispatcher.stop_stream(data['tag'])
        return '{ Success }'
    except Exception as e:
        print(e)
        return '{ Error 101 }'

@app.route('/api/add', methods=['POST'])
def add_stream():
    data = request.get_json()
    try:
        print(data)
        if not 'tag' in data:
            raise Exception('tag has to be contained in request data')
        dispatcher.add_stream(**data)
        return '{ Success }'
    except Exception as e:
        print(e)
        return '{ Error 101 }'

@app.route('/api/list', methods=['GET'])
def list_streams():
    try:
        streams = dispatcher.list_streams()
        return json.dumps(streams)
    except Exception as e:
        print(e)
        return '{ Error 101 }'

if __name__ == '__main__':
    # recommend.create_index()
    app.run(debug=True, host='0.0.0.0',port='6545')
