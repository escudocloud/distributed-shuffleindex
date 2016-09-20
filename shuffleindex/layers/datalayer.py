from abc import ABCMeta, abstractmethod

class DataLayer:
    __metaclass__ = ABCMeta

    @abstractmethod
    def get(self, key):
        pass

    @abstractmethod
    def put(self, key, value):
        pass
