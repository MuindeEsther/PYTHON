import os
import pytz
from datetime import datetime
import requests
import json
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

# Define the coordinates for Nairobi, Kenya
lat = -1.2921
lon = 36.8219

# config variable class
class Config:
    # get API key from environment variable
    api_key = os.environ.get("API_KEY")

# Construct the NWS API endpoint URL for the forecast
api_url = f"https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}"
#api_url = '#f'https://api.weather.gov/points/{latitude},{longitude}/forecast'

# Azure Blob Storage configuration
container_url = 'https://fermat01.blob.core.windows.net/weatherdata?sp=r&st=2023-11-01T13:32:44Z&se=2023-11-01T21:32:44Z&spr=https&sv=2022-11-02&sr=c&sig=u%2BI5r7w%2BlE0963ez%2F9%2FjNdvGMd90gxv0PWmCNITU4Nw%3D'  # Replace with your Azure Blob container URL
sas_token = 'sp=r&st=2023-11-01T13:32:44Z&se=2023-11-01T21:32:44Z&spr=https&sv=2022-11-02&sr=c&sig=u%2BI5r7w%2BlE0963ez%2F9%2FjNdvGMd90gxv0PWmCNITU4Nw%3D'  # Replace with your SAS token

try:
    # Make a GET request to the NWS API
    response = requests.get(api_url)
    response.raise_for_status()  # Check for any request errors

    # Parse the JSON response
    data = response.json()

    # Connect to Azure Blob Storage
    blob_service_client = BlobServiceClient(container_url, sas_token)
    container_client = blob_service_client.get_container_client('weatherdata')

    # Define the blob name
    blob_name = 'nairobi_weather.json'

    # Upload weather data to Azure Blob Storage
    blob_client = container_client.get_blob_client(blob_name)
    blob_client.upload_blob(json.dumps(data), overwrite=True)

    # Success message
    print("Weather data has been retrieved and stored in Azure Blob Storage.")
except requests.exceptions.RequestException as e:
    print(f"Error: {e}")
except KeyError:
    print("Error: Failed to retrieve and process weather data.")
