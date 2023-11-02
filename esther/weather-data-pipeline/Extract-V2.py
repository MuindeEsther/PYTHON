import os
import requests
import json
import pytz
from datetime import datetime
from azure.storage.blob import BlobServiceClient

# Azure Blob Storage configuration
container_url = 'https://fermat01.blob.core.windows.net/weatherdata'
sas_token = 'sp=r&st=2023-11-01T11:36:11Z&se=2023-11-01T19:36:11Z&spr=https&sv=2022-11-02&sr=c&sig=RB7SjDMB09sYaS7B0oV%2FTcLPurCXzMejlS0S9qgoWsY%3D'



# config varibles class
class Config:
    # get API key from environment variable
    api_key = os.environ.get("API_KEY")
    # get bucket name from environment
    bucket_name = os.environ.get("BUCKET_NAME")
    
    # Nairobi timezone
    nairobi_tz = pytz.timezone("Africa/Nairobi")
    # parts that unnecessary from API
    parts = "minutely,hourly,daily,alerts"
    # system of units
    units = "metric"

# Define get data from API function
def get_data(lat, lon):
    # Weather data API url
    weather_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&exclude={Config.parts}&units={Config.units}&appid={Config.api_key}"
    # air quality API url
    air_url = f"https://api.openweathermap.org/data/2.5/air_pollution?lat={lat}&lon={lon}&appid={Config.api_key}"
    
    # get a message payload from API
    weather_response = requests.get(weather_url)
    air_response = requests.get(air_url)
    
    # get dictionary result
    weather_result = weather_response.json()
    air_result = air_response.json()
    
    # return dictionary result
    return weather_result, air_result
    
    

def upload_to_blob_storage(data, container_url, sas_token, file_name):
    blob_service_client = BlobServiceClient(container_url, sas_token)
    
    # Convert result dictionary to JSON
    json_data = json.dumps(data)
    
    # Specify the blob name within the container
    blob_name = f"{weatherdata}.json"
    
    # Get a reference to the blob
    blob_client = blob_service_client.get_blob_client(container=container_url, blob=blob_name)
    
    # Upload the JSON data to the blob
    blob_client.upload_blob(json_data)
    
    
            
def main(event, context):
    # Get current date and time
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Coordinates of Nairobi
    coor = {
        "Nairobi": {"lat": '1.2921', "lon": '36.8219'}
    }
    
    result_dict = {}
    
    for district in coor:
        lat = coor[district]["lat"]
        lon = coor[district]["lon"]
        
        # Get weather data and air quality data
        weather_data, air_data = get_data(lat, lon)
        
        # append result to the result dictionary
        result_dict[f"{district}_data"] = {"weather": weather_data, "air": air_data}
        
    # call upload to cloud storage function
    upload_to_storage(f"raw_data_{current_date}.json", result_dict)
    
    # print text to logs
    print("The extracted part was run successfully.")
    
        

