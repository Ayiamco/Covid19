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


df=pd.read_csv("previous_data.csv")
print(df.head(50))
print("############################################################")
print(df.tail(50))
# print("###################################################################")
# #print(df.groupby(by="states").sum())
keys=json.load(open("rootkey.json","r"))
secret_key=keys["AWSSecretKey"]
access_key=keys["AWSAccessKeyId"]

#create s3 client
s3_client = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)

# Upload the file
try:
    response = download_file("previous_data.csv","joseph-covid19", "previous_data.csv")
except ClientError as e:
    logging.error(e)
print(response)






# from random import randint
# is_continue=True
# while is_continue:
#     while True:
#         try:
#             g=int(input('please guess the number: '))
#             if g >10:
#                print("INVALID Input")
#             else:
#                  break
#         except ValueError: 
#             print("Invalid Input!!!")

#             while True: 
#                 decision=input("do you want to continue: \nreply with y/n: ")
#                 if decision.lower() in ["y",'n']:
#                     break
#                 else:
#                     print("Invalid Input!!! \nreply with y/n: ")
            
#             if decision=='n':
#                 is_continue=False
#                 break
#     if is_continue==True:
#         guess=randint(1,10)

#         if g==guess:
#             print('True')
#             print('you guessed: ',g,"\ncomputers guess: ",guess)
#             break
#         else:
#             print('false')
#             print('you guessed: ',g,"\ncomputers guess: ",guess)
#             while True: 
#                 decision=input("do you want to continue: \nreply with y/n: ")
#                 if decision.lower() in ["y",'n']:
#                     break
#                 else:
#                     print("Invalid Input!!! \nreply with y/n: ")
#             if decision=='n':
#                 break

#$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$
# while True:
#     Days=input('enter a day in a week: ')
#     D=['monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'sunday']
#     if Days.lower() in D:
#         #that means the input is a day of the week
#         if Days.lower() in ["saturday","sunday"]:
#             print("Time to party!!!!!!!!!")
#             break
#         else:
#             print("My friend go to work.")
#             break
#     if Days.lower() not in D:
#             print('Not a day of the week')

            
        
    



