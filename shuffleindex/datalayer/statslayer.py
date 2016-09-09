from collections import Counter
from datalayer import DataLayer

class StatsLayer(DataLayer):

    def __init__(self, datalayer):
        self._datalayer = datalayer
        self.getcount =  Counter()
        self.putcount =  Counter()

    def get(self, key):
        self.getcount[key] += 1
        return self._datalayer.get(key)

    def put(self, key, value):
        self.putcount[key] += 1
        return self._datalayer.put(key, value)
