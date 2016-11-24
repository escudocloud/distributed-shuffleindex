from shuffleindex.layers.asynclayer import *
from shuffleindex.layers.datalayer import *
from time import time, sleep
import pytest

class MockLayer(DataLayer):

    def __init__(self, get_return=0, put_return=0, sleeptime=1):
        self._get_return = get_return
        self._put_return = put_return
        self._sleeptime = sleeptime

    def get(self, key):
        sleep(self._sleeptime)
        return self._get_return

    def put(self, key, value):
        sleep(self._sleeptime)
        return self._put_return


class TestPredicateLayer:

    def setup_method(self, test_method):
        self.sleeptime = 1
        self.memorylayer = MockLayer(0, 0, self.sleeptime)
        self.datalayer = AsyncLayer(self.memorylayer)

    def test_async_get(self):
        self.datalayer.get_send(0)
        sleep(self.sleeptime)                             # wait work completion
        start = time()
        value = self.datalayer.get_recv()
        assert (time() - start) < (self.sleeptime / 2.0)
        assert value == 0

    def test_async_put(self):
        self.datalayer.put_send(0, 0)
        sleep(self.sleeptime)                             # wait work completion
        start = time()
        result = self.datalayer.put_recv()
        assert (time() - start) < (self.sleeptime / 2.0)
        assert result == 0
