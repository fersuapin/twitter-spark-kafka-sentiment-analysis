# -*- coding: utf-8 -*-
"""
Created on Wed Mar 16 16:29:42 2022

@author: FerSu
"""
import tweepy
from tweepy import Stream, OAuthHandler
import json
# import boto3
# pip install boto3
import datetime 
import uuid
import pandas as pd
import argparse
from confluent_kafka import Producer
import socket
import logging


if __name__ == "__main__":
  logging.basicConfig(level=logging.WARN)

  parser = argparse.ArgumentParser()
  parser.add_argument("keywords", help="list of keywords to listen to")
  parser.add_argument("-b", "--broker",
                      help="server:port of the Kafka broker where messages will be published")
  parser.add_argument("-t", "--topic",
                      help="topic where messages will be published")
  args = parser.parse_args()
  
  producer = None
  topic = args.topic
  if args.broker != None:
    conf = {'bootstrap.servers': args.broker,
            'client.id': socket.gethostname()}
    producer = Producer(conf)


  consumer_key  = 'Wn8tWFuXaz6rlpoXcjrKEA5RR'

  consumer_secret = '2ib4WylruwbsAqpczybmJSOLEKQpkj77vXI1ozkBfGW1sVIQHl'

  Bearertoken = 'AAAAAAAAAAAAAAAAAAAAAAovaQEAAAAA%2ByPnmbolHrv%2Bt4usQtT3x%2BE%2B22M%3DzsoEBimo7wfED7ngMex0f6Moq2krNhH8ZwndOB6lJUUF2QLatO'

  access_token = '79878131-G8ATc4VtueBW9ilKAlNla4LPcMJ8NtOiPk5q0pB4F'

  access_token_secret = 'wxLoojiGci4a8RnMpbtAhTyUnKJWhG330xefkN24HLiei'

  auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
  auth.set_access_token(access_token, access_token_secret)

  class listener(tweepy.Stream):
      def on_status(self, status):
        producer.produce(topic, value = f"{status._json['created_at']} |\
{status._json['id']} |\
{status._json['id_str']} |\
{status._json['text']} |\
{status._json['source']} |\
{status._json['truncated']} |\
{status._json['in_reply_to_status_id']} |\
{status._json['in_reply_to_user_id']} |\
{status._json['user']['id']} |\
{status._json['user']['id_str']} |\
{status._json['user']['name']} |\
{status._json['user']['screen_name']} |\
{status._json['user']['location']} |\
{status._json['user']['url']} |\
{status._json['user']['description']} |\
{status._json['user']['protected']} |\
{status._json['user']['verified']} |\
{status._json['user']['followers_count']} |\
{status._json['user']['friends_count']} |\
{status._json['user']['listed_count']} |\
{status._json['user']['favourites_count']} |\
{status._json['user']['statuses_count']} |\
{status._json['user']['created_at']} |\
{status._json['user']['profile_image_url_https']} |\
{status._json['user']['default_profile']} |\
{status._json['user']['default_profile_image']} |\
{status._json['user']['withheld_in_countries']} |\
{status._json['user']['geo_enabled']} |\
{status._json['is_quote_status']} |\
{status._json['quote_count']} |\
{status._json['reply_count']} |\
{status._json['retweet_count']} |\
{status._json['favorite_count']} |\
{status._json['entities']['user_mentions']} |\
{status._json['entities']['hashtags']} |\
{status._json['entities']['urls']} |\
{status._json['entities']['symbols']} |\
{status._json['favorited']} |\
{status._json['retweeted']} |\
{status._json['filter_level']} |\
{status._json['lang']}")
        producer.flush()


  stream_tweet = listener(consumer_key, consumer_secret, access_token, access_token_secret)
  keywords = [args.keywords]

  l = stream_tweet.filter(track=keywords)

          
