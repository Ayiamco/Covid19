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
from flask import Flask,jsonify,request,render_template
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
    date=[date]*37
    states=["Gombe", "Bauchi", "Yobe", "Borno", "Adamawa", "Taraba","Jigawa", "Kano", "Katsina", "Kaduna", "Kebbi", "Zamfara", "Sokoto","Niger", "Benue", "Nassarawa", "Plateau", "Kogi","Kwara","Imo","Abia","Anambara","Ebonyi","Enugu","Oyo","Osun","Ogun","Lagos","Ekiti","Rivers","Cross River","Ondo",
    "Akwa Ibom","Delta","Edo","Bayelsa","Abuja FCT"]
    new_cases,active_cases,number_discharged,No_of_deaths=[],[],[],[]
    for state in states:
        if state in list(current_data.states):
            new_cases.append(current_data[current_data.states==state]["new_cases"])
            active_cases.append(current_data[current_data.states==state]["active_cases"])
            number_discharged.append(current_data[current_data.states==state]["number_discharged"])
            No_of_deaths.append(current_data[current_data.states==state]["No_of_deaths"])
        else:
            new_cases.append(0)
            number_discharged.append(0)
            No_of_deaths.append(0)

    #dataset
    df=pd.DataFrame({
        "states":states,
        "new_cases":new_cases,
        "number_discharged":number_discharged,
        "No_of_deaths":No_of_deaths
    })
    df["geopolitical_zone"]=df.states.apply(lambda x: add_geopolitical_zone(x))
    df["date"]=date
    new_previous_data=pd.concat([previous_data,df],axis=0,ignore_index=True)
    new_previous_data.to_csv("previous_data.csv",index=False)
    upload_file("previous_data.csv",s3_bucket_name)

@app.route('/')
def index():
    return render_template("index.html")
@app.route('/covid19',methods=['GET'])
def getdata():
    if request.method=="GET":
        bucket_name=os.getenv("s3")
        object_name="previous_data.csv"
        file_name="data_download.csv"
        success=False
        count=0
        while not success:
            count+=1
            if count>5:
                break
            success=download_file(bucket_name, object_name, file_name)
        return_data=pd.read_csv(file_name)
        return_data.to_json("return_data.json")
        return_data=json.load(open("return_data.json","r"))
        return jsonify(return_data)

if __name__ == "__main__":
    app.run(debug=False,use_reloader=False)