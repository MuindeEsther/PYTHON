!pip install
import json
import requests
from azure.storage.blob import BlobServiceClient
from datetime import datetime

# Azure Blob Storage configuration
container_url = 'https://fermat01.blob.core.windows.net/weatherdata'  # Replace with your Azure Blob container URL
sas_token = 'sp=r&st=2023-11-01T11:36:11Z&se=2023-11-01T19:36:11Z&spr=https&sv=2022-11-02&sr=c&sig=RB7SjDMB09sYaS7B0oV%2FTcLPurCXzMejlS0S9qgoWsY%3D' # Replace with your SAS token

# Define the API endpoint to fetch weather data
api_url = 'https://www.weather.gov/documentation/services-web-api#:~:text=The%20National%20Weather%20Service%20(NWS,upon%20the%20information%20life%20cycle'  # Replace with your weather API endpoint

# Function to get data from the weather API
def get_weather_data(api_url):
    response = requests.get(api_url)
    if response.status_code == 200:
        return response.json()
    else:
        return None

# Function to upload data to Azure Blob Storage
def upload_to_blob_storage(data, container_url, sas_token, file_name):
    blob_service_client = BlobServiceClient(container_url, sas_token)
    
    # Specify the blob name within the container
    blob_name = f"{file_name}.json"
    
    # Get a reference to the blob
    blob_client = blob_service_client.get_blob_client(blob=blob_name)
    
    # Upload the JSON data to the blob
    blob_client.upload_blob(json.dumps(data), overwrite=True)
    print(f"Data uploaded to {blob_name} in Azure Blob Storage.")

def main():
    # Generate a unique file name based on the current date and time
    current_date = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_name = f"weather_data_{current_date}"
    
    # Get weather data from the API
    weather_data = get_weather_data(api_url)
    
    if weather_data:
        # Upload weather data to Azure Blob Storage
        upload_to_blob_storage(weather_data, container_url, sas_token, file_name)
    else:
        print("Failed to fetch weather data from the API.")
    
if __name__ == "__main__":
    main()
