from shuffleindex.datalayer.authencryptionlayer import *
from shuffleindex.datalayer.predicatelayer import *
from shuffleindex.datalayer.memorylayer import *
import pytest

class TestAuthEncryptionLayer:

    def setup_method(self, test_method):
        key = '0123456789ABCDEF'
        self.memorylayer = MemoryDataLayer()
        self.predicatelayer = PredicateLayer(self.memorylayer)
        self.datalayer = AuthEncryptionLayer(self.predicatelayer, key)

    def test_encryption(self):
        key, value = 1, 'Hello, World!'
        self.predicatelayer.put_predicate = lambda k, v: k == key and v != value
        self.datalayer.put(key, value)
        assert value == self.datalayer.get(key)

    def tamper_data(self, key, offset):
        bytes = bytearray(self.memorylayer.get(key))
        bytes[offset] = bytes[offset] ^ 0x01
        self.memorylayer.put(key, str(bytes))

    def test_tamper_nonce(self):
        key, value = 1, 'Hello, World!'
        self.datalayer.put(key, value)
        self.tamper_data(1, 0)
        with pytest.raises(ValueError):
            self.datalayer.get(key)

    def test_tamper_tag(self):
        key, value = 1, 'Hello, World!'
        self.datalayer.put(key, value)
        self.tamper_data(1, 16)
        with pytest.raises(ValueError):
            self.datalayer.get(key)

    def test_tamper_ciphertext(self):
        key, value = 1, 'Hello, World!'
        self.datalayer.put(key, value)
        self.tamper_data(1, 32)
        with pytest.raises(ValueError):
            self.datalayer.get(key)
