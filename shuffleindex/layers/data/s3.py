from ..datalayer import DataLayer
from boto.s3.connection import S3Connection
from boto.s3.key import Key

class S3DataLayer(DataLayer):

    def __init__(self, access_key, secret_key, bucket_name):
        self._connection = S3Connection(access_key, secret_key)
        self._bucket = self._connection.get_bucket(bucket_name)

    def get(self, key):
        k = Key(self._bucket)
        k.key = key
        return k.get_contents_as_string()

    def put(self, key, value):
        k = Key(self._bucket)
        k.key = key
        k.set_contents_from_string(value)
        return key
