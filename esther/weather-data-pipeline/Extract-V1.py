# This is the first version of the pipeline function
# import libraries
import os
import json

import requests
from datetime import datetime
import pytz
from azure.storage.filedatalake import DataLakeStoreClient

# config variable class
class Config:
    # get API key from environment variable
    api_key = os.environ.get("API_KEY")
    # get bucket name from environment variable
    bucket_name = os.environ.get("BUCKET_NAME")
    
    # Nairobi timezone
    nairobi_tz = pytz.timezone("Africa/Kenya")
    # parts that are unnecessary from API
    parts = "minutes,hourly,daily,alerts"
    # system of units
    units = "metric"

# define get data from API function
def get_data(lat, lon):
    # weather data API url
    weather_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={Config.parts}&units={Config.units}&appid={Config.api_key}"
    air_url =  f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={Config.api_key}"
    
    # get a message payload from API
    weather_response = requests.get(weather_url)
    air_response = requests.get(air_url)
    
    # get dictionary result
    weather_result = weather_response.json()
    air_result = air_response.json()
    
    # return dictionary result
    return weather_result, air_result

# define upload raw data to cloud storage function
# docs =>  https://cloud.google.com/storage/docs/uploading-objects-from-memory

def upload_to_storage(file_name, data):
    storage_client = storage.Client()
    bucket = storage_client.bucket(Config.bucket_name)
    
    # convert result dictionary to json
    json_data = json.dumps(data)
    
    # specify folder - processing (waiting for transforming)
    blob = bucket.blob(f"processsing/{file_name}")
    blob.upload_from_string(json_data)
    
#define main function to entry run
def main(event, context):
    # get current date and time in East African Timezone
    current_date = datetime.now(Config.nairobi_tz).strftime("%Y%m%d_%H%M%S")
    
    # Coordinates of Nairobi
    coor = {
        "Nairobi":{"lat": '1.2921',"lon": '36.8219'}
        }
    
    # define empty result dictionary
    result_dict = {}
    
    # for loop to get data from API
    for district in coor:
        lat = coor[district]['lat']
        lon = coor[district]["lon"]
        
        # get weather data and air quality data
        weather_data, air_data = get_data(lat, lon)
        
        # append result to the result dictionary
        result_dict[f"{district}_data"] = {"weather": weather_data, "air": air_data}
        
    # call upload to cloud storage function
    upload_to_storage(f"raw_data_{current_date}.json", result_dict)
    
    # print text to logs
    print("The extracted part was run successfully.")
    