from Crypto.Random import get_random_bytes
from Crypto.Cipher import AES
from Crypto.Util import Counter
from datalayer import DataLayer

class EncryptionLayer(DataLayer):

    def __init__(self, datalayer, key):
        self._datalayer = datalayer
        self._key = key

    def _make_cipher(self, iv=None):
        """Construct a new AES object every time the method is invoked"""
        iv = iv or get_random_bytes(16)
        ctr = Counter.new(128, initial_value=long(iv.encode('hex'), 16))
        return AES.new(self._key, AES.MODE_CTR, counter=ctr), iv

    def get(self, key):
        value = self._datalayer.get(key)
        iv, ciphertext = value[:16], value[16:]
        cipher, _ = self._make_cipher(iv)
        return cipher.decrypt(ciphertext)

    def put(self, key, value):
        cipher, iv = self._make_cipher()
        ciphertext = cipher.encrypt(value)
        return self._datalayer.put(key, iv + ciphertext)
