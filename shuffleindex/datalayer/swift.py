from datalayer import DataLayer

class SwiftDataLayer(DataLayer):

    def __init__(self):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def put(self, key, value):
        raise NotImplementedError
