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
sched.add_job(lambda : start_cron(),trigger='interval',hours=0.01)
sched.start()
def start_cron():
    #scrap tables from the webpage
    multiple_tables=pd.read_html("https://covid19.ncdc.gov.ng/")
    summary_table=multiple_tables[2] #
    current_data=multiple_tables[3] #summarized data of what has happened so far

    #rename column fields
    current_data.columns=["states","new_cases","active_cases","number_discharged","No_of_deaths"]

    #drop unwanted rows
    current_data=current_data[current_data.states!="Total"]
    current_data=current_data[current_data.states.notnull()]

    current_data_states=list(current_data.states)
    all_states=['Abia', 'Abuja FCT', 'Adamawa', 'Akwa Ibom', 'Anambra', 'Bauchi', 'Bayelsa', 'Benue', 'Borno', 'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu', 'Gombe', 'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Kogi', 'Kwara', 'Lagos', 'Nasarawa', 'Niger', 'Ogun', 'Ondo', 'Osun', 'Oyo', 'Plateau', 'Rivers', 'Sokoto', 'Taraba', 'Yobe', 'Zamfara',"Total"]
    my_dict={"states":[],"new_cases":[],"active_cases":[],"number_discharged":[],"No_of_deaths":[]}
    for state in all_states:
        if state not in current_data_states:
            my_dict["states"].append(state)
            my_dict["new_cases"].append(0)
            my_dict["active_cases"].append(0)
            my_dict["number_discharged"].append(0)
            my_dict["No_of_deaths"].append(0)
    if len(my_dict["states"])>0:
        #corona virus cases have not being confirmed in all states
        df=pd.DataFrame(my_dict)
        current_data=pd.concat([df,current_data],ignore_index=True,axis=0)
        current_data.index=list(range(0,37))

    #add geopolitical zones for each record
    current_data["geopolitical_zone"]=current_data.states.apply(lambda x: add_geopolitical_zone(x))
    print(current_data)

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
    print("Got current data")
    date=str(datetime.datetime.today()).split(" ")[0]
    date=[date]*37
    current_data["date"]=date
    new_previous_data=pd.concat([previous_data,current_data],axis=0,ignore_index=True)
    new_previous_data.to_csv("previous_data.csv",index=False)
    #upload_file("previous_data.csv",s3_bucket_name)
    print(new_previous_data)
    print("Done.........................................................")

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