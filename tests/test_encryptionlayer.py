from shuffleindex.datalayer.encryptionlayer import *
from shuffleindex.datalayer.predicatelayer import *
from shuffleindex.datalayer.memorylayer import *

class TestEncryptionLayer:

    def setup_method(self, test_method):
        key = '0123456789ABCDEF'
        self.memorylayer = MemoryDataLayer()
        self.predicatelayer = PredicateLayer(self.memorylayer)
        self.datalayer = EncryptionLayer(self.predicatelayer, key)

    def test_encryption(self):
        key, value = 1, 'Hello, World!'
        self.predicatelayer.put_predicate = lambda k, v: k == key and v != value
        self.datalayer.put(key, value)
        assert value == self.datalayer.get(key)
