# Twitter Sentiment Analysis on Streaming Data with Apache Spark and Kafka

## Instructions

Start all necessary services:

sudo service mariadb start
sudo service kafka start

## Create the Database and Table where you want to load the data in MariaDB

The .sql file "tweets_db" creates a new database called "twitter" and a Table "Tweets"

## Start the Python/Kafka producer with the topic you want to ingest. Ex. Uber

## ACCESS TOKENS HAVE BEEN REVOKED FOR THIS FILE, YOU WILL HAVE TO EDIT THESE WITH YOUR OWN IN ORDER FOR YOuR CODE TO RUN

python3 twitter_producer.py uber -b localhost:9092 -t twitter

## Run the Spark Notebook

## Query the data from MariaDB using your visualization application of choice by connecting to the Database

