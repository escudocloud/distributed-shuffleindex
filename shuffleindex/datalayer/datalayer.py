from abc import ABCMeta, abstractmethod

class DataLayer:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value):
        pass


class MemoryDataLayer(DataLayer):

    def __init__(self):
        self._dict = dict()

    def get(self, key):
        return self._dict.__getitem__(key)

    def put(self, key, value):
        return self._dict.__setitem__(key, value)
