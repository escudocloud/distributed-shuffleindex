from shuffleindex.layers.data.swift import SwiftDataLayer
from swiftclient.exceptions import ClientException
import pytest

try:
    from configs.swift import *
except ImportError:
    pytestmark = pytest.mark.skipif(True, reason='missing configuration file')


data = [(1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'd')]


class TestSwift:

    def setup_method(self, test_method):
        self.swift = SwiftDataLayer(authurl, user, password,
                                    tenant_name, container_name)

    def _put(self):
        for i in range(len(data)):
            self.swift.put(data[i][0], data[i][1])

    def test_get_existing_keys(self):
        self._put()
        for i in range(len(data)):
            assert self.swift.get(data[i][0])[1] == data[i][1]

    def test_get_missing_keys(self):
        self._put()
        keys = [10, 15]
        for k in keys:
            with pytest.raises(ClientException):
                self.swift.get(k)

    def teardown_method(self, test_method):
        self.swift.clean()
