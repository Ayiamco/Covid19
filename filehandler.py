import requests
import os
import boto3
import pandas as pd
import logging
import boto3
import json
from botocore.exceptions import ClientError


def upload_file(file_name, bucket_name, object_name=None):
    """Upload a file to an S3 bucket
    :param file_name: File to upload
    :param bucket: Bucket to upload to
    :param object_name: S3 object name. If not specified then file_name is used
    :return: True if file was uploaded, else False
    """

    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = file_name

    #load access keys
    #access_key=os.getenv("AWSAccessKeyId")
    #secret_key=os.getenv("AWSSecretKey")
    keys=json.load(open("rootkey.json","r"))
    secret_key=keys["AWSSecretKey"]
    access_key=keys["AWSAccessKeyId"]

    #create s3 client
    s3_client = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)
    
    # Upload the file
    try:
        response = s3_client.upload_file(file_name, bucket_name, object_name)
    except ClientError as e:
        logging.error(e)
        return False
    return True

def download_file(bucket_name, object_name, file_name):
    #load access keys
    # access_key=os.getenv("AWSAccessKeyId")
    # secret_key=os.getenv("AWSSecretKey")
    keys=json.load(open("rootkey.json","r"))
    secret_key=keys["AWSSecretKey"]
    access_key=keys["AWSAccessKeyId"]
    
    
    #create s3 client
    s3_client = boto3.client('s3',aws_access_key_id=access_key,aws_secret_access_key=secret_key)

    try:
        response = s3_client.download_file(bucket_name, object_name, file_name)
    except :
        return False
    return True
    

def add_geopolitical_zone(state):
    """Add geopolitical zone based on the state"""
    south_south=["rivers","crossriver","cross-river","cross river",
    "akwaibom","akwa ibom","akwa-ibom","delta","edo","bayelsa"]
    if state.lower() in ["gombe", "bauchi", "yobe", "borno", "adamawa", "taraba"]:
        return "NorthEast"
    elif state.lower() in ["jigawa", "kano", "katsina", "kaduna", "kebbi", "zamfara", "sokoto"]:
        return "NorthWest"
    elif state.lower() in ["niger", "benue", "nassarawa", "plateau", "kogi","kwara"]:
        return "NorthCentral"
    elif state.lower() in south_south:
        return "SouthSouth"
    elif state.lower() in ["imo","abia","anambara","ebonyi","enugu"]:
        return "SouthEast"
    elif state.lower() in ["oyo","osun","ogun","lagos","ekiti","ondo"]:
        return "SouthWest"
    else:
        return None

def get_todays_data(current_data,grouped_previous_data):
    """
    New Incoming data is a sum of of all that has happened.
    To get what happened today alone we need to subtract the previous data
    from the new incoming sumarized data.
    """
    for field in ["new_cases","active_cases","number_discharged","No_of_deaths"]:
        if current_data[field]==grouped_previous_data.loc[current_data["states"]][field]:
            #previous data is same as current data
            pass
        else:
            #current data for particular field is greater than previous data
            slicer=current_data["states"]
            current_data[field]=current_data[field]-grouped_previous_data.loc[slicer][field]
    return current_data

