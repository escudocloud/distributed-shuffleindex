from Crypto.Cipher import AES
from datalayer import DataLayer

class AuthEncryptionLayer(DataLayer):

    def __init__(self, datalayer, key):
        self._datalayer = datalayer
        self._key = key

    def get(self, key):
        value = self._datalayer.get(key)
        nonce, tag, ciphertext = value[:16], value[16:32], value[32:]
        cipher = AES.new(self._key, AES.MODE_EAX, nonce)
        return cipher.decrypt_and_verify(ciphertext, tag)

    def put(self, key, value):
        cipher = AES.new(self._key, AES.MODE_EAX)
        ciphertext, tag = cipher.encrypt_and_digest(value)
        return self._datalayer.put(key, cipher.nonce + tag + ciphertext)
