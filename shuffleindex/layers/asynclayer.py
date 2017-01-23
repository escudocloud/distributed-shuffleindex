from datalayer import DataLayer
from concurrent.futures import ThreadPoolExecutor

class AsyncLayer(DataLayer):

    def __init__(self, datalayer=None):
        self._datalayer = datalayer
        self._pool = ThreadPoolExecutor(max_workers=1)

    def get(self, key):
        return self._pool.submit(self._datalayer.get, key)

    def put(self, key, value):
        return self._pool.submit(self._datalayer.put, key, value)
