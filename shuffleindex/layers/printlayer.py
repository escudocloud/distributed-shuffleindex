from datalayer import DataLayer

class PrintLayer(DataLayer):

    def __init__(self, datalayer=None):
        self._datalayer = datalayer

    def get(self, key):
        value = self._datalayer.get(key) if self._datalayer else None
        print('GET\nkey=%s\nvalue=%s' % (key, value))
        return value

    def put(self, key, value):
        result = self._datalayer.put(key, value) if self._datalayer else None
        print('PUT\nkey=%s\nvalue=%s\nresult=%s' % (key, value, result))
        return result
