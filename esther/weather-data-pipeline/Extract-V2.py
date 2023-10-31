import os
import requests
import json
import pytz
from datetime import datetime
from azure.storage.filedatlake import DataLakeStoreClient

# Azure Data Lake Storage Gen2 configuration
storage_account_name = 'fermat01'
storage_account_key = ''
file_system_name = ''
directory_name = ''

# Azure Synapse Analytics connection
synapse_connection_string =''

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
    
    

def upload_to_storage(file_name, data):
    data_lake_client = DataLakeStoreClient(account_url=f"")
    
    # Convert result dictionary to JSON
    json_data = json.dumps(data)
    
    # Specify the target path in Data Lake Storage Gen2
    destination_path = ''
    
    # Write the data to a JSON file in Data Lake Storage Gen2
    with data_lake_client.get_file_client(file_system=file_system_name, directory=directory_name, file=file_name) as file_client:
        with file_client.create_file() as file:
            file.write(json_data)
            
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
    
        

