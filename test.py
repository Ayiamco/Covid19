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

df=pd.read_csv("covid19_data.csv",parse_dates=False)
print(df.columns)
df.drop("Unnamed: 0",axis=1,inplace=True)
df.columns=['states', 'geopolitical_zone', 'new_cases', 'number_discharged',
       'No_of_deaths', 'date']
df1=df.groupby("date").sum()
df1["date"]=df1.index
holder_list=list(df1.new_cases)
accum_sum=[holder_list[0]]
for i,j in enumerate(holder_list,start=0):
    if i!=0:
        accum_sum.append(accum_sum[-1]+j)
df1["new_cases"]=accum_sum

df1["states"]=["Total"]*len(df1)
df1.index=list(range(0,len(df1)))
print(df1.head(50))
import numpy as np
count=1
for date in df.date.unique():
    dummy_df1=df[df.date==date]
    index=np.array(list(range(0,37)))+38
    dummy_df1.index=index
    dummy_df2=df1[df1.date==date]
    dummy_df2.index=np.array([37])+38
    if count==1:
        my_df=dummy_df1
        my_df=pd.concat([my_df,dummy_df2],ignore_index=False,axis=0)
    else:
        my_df=pd.concat([my_df,dummy_df1,dummy_df2],ignore_index=False,axis=0)
    count+=1
#print(my_df.groupby("states").sum())


