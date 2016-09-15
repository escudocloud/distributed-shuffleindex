from Crypto.Cipher import AES
from Crypto.Util import Counter
from datalayer import DataLayer

class EncryptionLayer(DataLayer):

    def __init__(self, datalayer, key, iv):
        self._datalayer = datalayer
        self._key, self._iv, = key, iv

    def _make_cipher(self):
        """Construct a new AES object every time the method is invoked"""
        ctr = Counter.new(128, initial_value=long(self._iv.encode("hex"), 16))
        return AES.new(self._key, AES.MODE_CTR, counter=ctr)

    def get(self, key):
        ciphertext = self._datalayer.get(key)
        return self._make_cipher().decrypt(ciphertext)

    def put(self, key, value):
        ciphertext = self._make_cipher().encrypt(value)
        return self._datalayer.put(key, ciphertext)
