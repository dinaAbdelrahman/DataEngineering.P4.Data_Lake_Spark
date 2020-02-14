# Introduction

A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

# Data Source

The available data consists of 2 parts:
* a directory of JSON logs on user activity on the app (which songs were requested by users, the log timestamp, some users information ) 
* a directory with JSON  on the songs in their app (songs information as well as artists)

The two datasets that reside in S3 as per below:
* Song data: s3://udacity-dend/song_data
* Log data: s3://udacity-dend/log_data

# Project Description

I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

# Overview for the structure of the tables

Below shows the table  I will building from the 2 datasets, details of the dimesions table creation can be found in etl.py

![image.png](attachment:image.png)

# Project Steps:

The steps details are in etl.py
1. Import the song files from s3://udacity-dend/song_data
2. Adjust the schema on read with appropriate data type
3. Construct the 2 facts tables artists and songs
4. Write the artists and songs into S3 using parquet format
5. Import the song files from s3://udacity-dend/log_data
6. Construct the 2 facts tables users and time
7. Write the users and time into S3 using parquet format
8. Construct the fact table songplay by joining the 2 tables songs and logs
9. Write the songplay table into S3 using parquet format

## Running the project Guidlines

1. Update the AWS credentials in the dl.cfg
2. Run the etl.py
3. Check the S3 bucket and verify the parquet files were properly generated
