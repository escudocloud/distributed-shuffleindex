from ..datalayer import DataLayer
from oauth2client.client import GoogleCredentials
from google.cloud import storage


class GCSDataLayer(DataLayer):

    def __init__(self, json_key_service_filename, project_name, bucket_name):
        """Creates the service object for calling the Cloud Storage API."""
        credentials = GoogleCredentials.from_stream(json_key_service_filename)
        # Create connection
        self.client = storage.Client(project=project_name,
                                     credentials=credentials)
        self.bucketname = bucket_name

    def get(self, key):
        bucket = self.client.get_bucket(self.bucketname)
        blob = bucket.get_blob(key)
        return blob.download_as_string()

    def put(self, key, value):
        bucket = self.client.get_bucket(self.bucketname)
        blob = bucket.blob(key)
        blob.upload_from_string(value)

    def clean(self):
        bucket = self.client.get_bucket(self.bucketname)
        for b in bucket.list_blobs():
            b.delete()
