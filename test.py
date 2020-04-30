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

# df=pd.read_csv("covid19_data.csv",parse_dates=False)
# print(df.columns)
# df.drop("Unnamed: 0",axis=1,inplace=True)
# df.columns=['states', 'geopolitical_zone', 'new_cases', 'number_discharged',
#        'No_of_deaths', 'date']
# df1=df.groupby("date").sum()
# df1["date"]=df1.index
# holder_list=list(df1.new_cases)
# accum_sum=[holder_list[0]]
# for i,j in enumerate(holder_list,start=0):
#     if i!=0:
#         accum_sum.append(accum_sum[-1]+j)
# df1["new_cases"]=accum_sum

# df1["states"]=["Total"]*len(df1)
# df1.index=list(range(0,len(df1)))
# print(df1.head(50))
# import numpy as np
# count=1
# for date in df.date.unique():
#     dummy_df1=df[df.date==date]
#     index=np.array(list(range(0,37)))+38
#     dummy_df1.index=index
#     dummy_df2=df1[df1.date==date]
#     dummy_df2.index=np.array([37])+38
#     if count==1:
#         my_df=dummy_df1
#         my_df=pd.concat([my_df,dummy_df2],ignore_index=False,axis=0)
#     else:
#         my_df=pd.concat([my_df,dummy_df1,dummy_df2],ignore_index=False,axis=0)
#     count+=1
# my_df.to_csv("covid19_data2.csv")
# keys=json.load(open("rootkey.json","r"))
# s3_bucket_name=keys["s3"]

# #load previous data
# previous_data=download_file(s3_bucket_name,"previous_data.csv","previous_data.csv")
# previous_data=pd.read_csv("test_data.csv",header=0)
# previous_data.drop("Unnamed: 0",axis=1,inplace=True)
# print(previous_data.columns)
# grouped_previous_data=previous_data.groupby(
#     by="States Affected").sum()
# grouped_previous_data.drop("Total",inplace=True,axis=0)
# previous_data.to_csv("test_data.csv",index=False)
# print("         Grouped data")
# print(grouped_previous_data)
df=pd.read_csv("previous_data.csv")
print("getting started...........")
resp=requests.get("https://covid19.ncdc.gov.ng/")
multiple_tables=pd.read_html(resp.text,parse_dates=False)
summary_table=multiple_tables[2] #
current_data=multiple_tables[3] #summarized data of what has happened so far
print("got tables")