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
from flask import Flask
from apscheduler.schedulers.background import BackgroundScheduler
import atexit
import datetime
import json

app=Flask(__name__)

sched = BackgroundScheduler(daemon=True)
sched.add_job(lambda : start_cron(),trigger='interval',hours=24)
sched.start()
def start_cron():
    #scrap tables from the webpage
    multiple_tables=pd.read_html("https://covid19.ncdc.gov.ng/")
    summary_table=multiple_tables[2] #
    current_data=multiple_tables[3] #summarized data of what has happened so far

    #rename column fields
    current_data.columns=["states","new_cases","active_cases","number_discharged","No_of_deaths"]

    #add geopolitical zones for each record
    current_data["geopolitical_zone"]=current_data.states.apply(lambda x: add_geopolitical_zone(x))

    #get s3 bucket name
    #s3_bucket_name=os.getenv("s3_bucket")
    keys=json.load(open("rootkey.json","r"))
    s3_bucket_name=keys["s3"]

    #load previous data
    previous_data=download_file(s3_bucket_name,"previous_data.csv","previous_data.csv")
    previous_data=pd.read_csv("previous_data.csv",header=0)
    grouped_previous_data=previous_data.groupby(
        by="states").sum()

    current_data=current_data.apply(lambda x:get_todays_data(x,grouped_previous_data),axis=1)
    date=str(datetime.datetime.today()).split(" ")[0]
    date=[date]*current_data.shape[0]
    #current_data.to_csv("previous_data.csv",index=False)  
    #success=upload_file("previous.csv","joseph-covid19")
    print(current_data.columns)


if __name__ == "__main__":
    app.run(debug=False,use_reloader=False)