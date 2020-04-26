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

multiple_tables=pd.read_html("https://covid19.ncdc.gov.ng/")
summary_table=multiple_tables[2] #
current_data=multiple_tables[3] #summarized data of what has happened so far

#rename column fields
current_data.columns=["states","new_cases","active_cases","number_discharged","No_of_deaths"]
print(current_data)
print("#################################################################################################")
#add geopolitical zones for each record
current_data["geopolitical_zone"]=current_data.states.apply(lambda x: add_geopolitical_zone(x))

#get s3 bucket name
#s3_bucket_name=os.getenv("s3_bucket")
keys=json.load(open("rootkey.json","r"))
s3_bucket_name=keys["s3"]



#current_data=current_data.apply(lambda x:get_todays_data(x,grouped_previous_data),axis=1)
date=str(datetime.datetime.today()).split(" ")[0]
date=[date]*37
states=['Abia', 'Abuja FCT', 'Adamawa', 'Akwa Ibom', 'Anambara', 'Bauchi', 'Bayelsa', 'Benue', 'Borno', 'Cross River', 'Delta', 'Ebonyi', 'Edo', 'Ekiti', 'Enugu', 'Gombe', 'Imo', 'Jigawa', 'Kaduna', 'Kano', 'Katsina', 'Kebbi', 'Kogi', 'Kwara', 'Lagos', 'Nassarawa', 'Niger', 'Ogun', 'Ondo', 'Osun', 'Oyo', 'Plateau', 'Rivers', 'Sokoto', 'Taraba', 'Yobe', 'Zamfara']
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
print(len(states),len(new_cases),len(number_discharged),len(No_of_deaths))
df=pd.DataFrame({
    "states":states,
    "new_cases":new_cases,
    "number_discharged":number_discharged,
    "No_of_deaths":No_of_deaths
})
print(list(df.new_cases))
print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
#df["geopolitical_zone"]=df.states.apply(lambda x: add_geopolitical_zone(x))
#df["date"]=date
#new_previous_data=pd.concat([previous_data,df],axis=0,ignore_index=True)
#df.to_csv("previous_data.csv",index=False)
#upload_file("previous_data.csv","joseph-covid19","previous_data25.csv")
print(df.head())

