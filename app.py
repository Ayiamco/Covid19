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
import time
import datetime
import json

app=Flask(__name__)

sched = BackgroundScheduler(daemon=True)
sched.add_job(lambda : start_cron(),trigger='interval',hours=0.01)
sched.start()
def start_cron():
    #scrap tables from the webpage
    while True:
        try:
            print("getting started...........")
            resp=requests.get("https://covid19.ncdc.gov.ng/")
            multiple_tables=pd.read_html(resp.text,parse_dates=False)
            summary_table=multiple_tables[2] #
            current_data=multiple_tables[3] #summarized data of what has happened so far
            print("got tables")
            #load previous data
            #get s3 bucket name
            #s3_bucket_name=os.getenv("s3_bucket")
            keys=json.load(open("rootkey.json","r"))
            s3_bucket_name=keys["s3"]
            #previous_data=download_file(s3_bucket_name,"previous_data.csv","previous_data.csv")
            previous_data=pd.read_csv("previous_data.csv",header=0)
            break
        except:
            print("network error")
            time.sleep(5)
    
    #rename column fields
    current_data.columns=["states","new_cases","No. of Active Cases","number_discharged","No_of_deaths"]

    #drop unwanted rows  and rows where states is null 
    Total=current_data[current_data.states=="Total"]
    current_data=current_data[current_data.states!="Total"]
    current_data=current_data[current_data.states.notnull()]

    current_data_states=list(current_data.states)
    all_states=['Abia', 'Abuja FCT', 'Adamawa', 'Akwa Ibom', 'Anambra', 'Bauchi',
     'Bayelsa', 'Benue', 'Borno', 'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti',
      'Enugu', 'Gombe', 'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 
      'Kogi', 'Kwara', 'Lagos', 'Nasarawa', 'Niger', 'Ogun', 'Ondo', 'Osun', 'Oyo', 
      'Plateau', 'Rivers', 'Sokoto', 'Taraba', 'Yobe', 'Zamfara']
    my_dict={"states":[],"new_cases":[],"No. of Active Cases":[],
    "number_discharged":[],"No_of_deaths":[]}
    for state in all_states:
        if state not in current_data_states:
            my_dict["states"].append(state)
            my_dict["new_cases"].append(0)
            my_dict["No. of Active Cases"].append(0)
            my_dict["number_discharged"].append(0)
            my_dict["No_of_deaths"].append(0)
    if len(my_dict["states"])>0:
        #corona virus cases have not being confirmed in all states
        df=pd.DataFrame(my_dict)
        current_data=pd.concat([df,current_data],ignore_index=True,axis=0)
        current_data.index=list(range(0,37))
    
    #add geopolitical zones for each record
    current_data["geopolitical_zone"]=current_data.states.apply(lambda x: add_geopolitical_zone(x))
    Total["geopolitical_zone"]= [None]
    
    #create summary table of previous data sum accross states
    grouped_previous_data=previous_data.groupby(
        by="states").sum()
    grouped_previous_data.drop("Total",inplace=True,axis=0)

    #get the data for yesterday
    current_data=current_data.apply(lambda x:get_todays_data(x,grouped_previous_data),axis=1)
    current_data.sort_values(by="states",inplace=True)
    
    #add the accumulated total to yesterdays data
    current_data=pd.concat([current_data,Total],ignore_index=True)
    
    #add timestamp to yesterday data
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    current_data["date"]=[str(yesterday)]*38

    #add yesterdays data to previous data
    new_previous_data=pd.concat([previous_data,current_data],axis=0,ignore_index=True)
    new_previous_data.to_csv("previous_data_trial.csv",index=False)

    #push combined data to storage
    # is_successful=False
    # while is_successful==False:
    #     try:
    #         is_successful=upload_file("previous_data.csv",s3_bucket_name)
    #     except:
    #         time.sleep(18)
    
    print(new_previous_data.tail(50))
    print("Done.........................................................")
    print(new_previous_data.groupby(by="states").sum())

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