import requests
import os
import boto3
import pandas as pd
import logging
import boto3
from botocore.exceptions import ClientError
from filehandler import upload_file
from filehandler import add_geopolitical_zone
from filehandler import download_file
from filehandler import get_todays_data
from filehandler import get_todays_data
import flask
import json 
import datetime


df=pd.read_csv("previous_data_trial.csv")
print(df.head(50))
print("############################################################")
print(df.tail(50))
print("###################################################################")
print(df.groupby(by="states").sum())