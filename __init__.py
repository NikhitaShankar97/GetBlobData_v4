import logging
import azure.functions as func
from azure.storage.blob import BlobServiceClient
import pandas as pd
from io import StringIO

# Azure Blob Storage connection string
BLOB_CONNECTION_STRING = "DefaultEndpointsProtocol=https;AccountName=nikhita4blobstorage;AccountKey=qzCJ+YKxqcJfV8ivjMxZ4R2deOe0IDUdCCUQpsuTXAXLtIJRVP/Wb7xVPNmzj9xj+JxNn32LJXrE+AStkuhulw==;EndpointSuffix=core.windows.net"
CONTAINER_NAME = "nikhita4blobcontainer"

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Processing HTTP request...")

    # Get the file name from query parameters
    file_name = req.params.get('file')
    if not file_name:
        logging.error("File name not provided in query parameters.")
        return func.HttpResponse(
            "Please provide the file name in the query string using 'file'.",
            status_code=400
        )

    try:
        # Connect to Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(BLOB_CONNECTION_STRING)
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=file_name)

        logging.info(f"Attempting to fetch file: {file_name} from container: {CONTAINER_NAME}")

        # Download the blob content
        blob_data = blob_client.download_blob().readall()
        logging.info(f"Successfully downloaded file: {file_name}")

        # Load the blob content into a DataFrame
        csv_data = StringIO(blob_data.decode('utf-8'))
        df = pd.read_csv(csv_data)

        # Convert DataFrame to JSON and return
        json_data = df.to_json(orient="records")
        return func.HttpResponse(json_data, mimetype="application/json")

    except Exception as e:
        logging.error(f"Error occurred while processing file: {e}")
        return func.HttpResponse(f"Error processing file: {str(e)}", status_code=500)
