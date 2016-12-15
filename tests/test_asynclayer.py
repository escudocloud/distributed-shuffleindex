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
        future = self.datalayer.get(0)
        sleep(self.sleeptime)                             # wait work completion
        start = time()
        assert future.result() == 0
        assert (time() - start) < (self.sleeptime / 2.0)

    def test_async_put(self):
        future = self.datalayer.put(0, 0)
        sleep(self.sleeptime)                             # wait work completion
        start = time()
        assert future.result() == 0
        assert (time() - start) < (self.sleeptime / 2.0)
