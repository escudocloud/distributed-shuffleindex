from datalayer import DataLayer

class S3DataLayer(DataLayer):

    def __init__(self):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def put(self, key, value):
        raise NotImplementedError
