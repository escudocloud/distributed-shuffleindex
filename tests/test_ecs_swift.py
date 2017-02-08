import pytest

try:
    from configs.ecs_swift import *
    from shuffleindex.layers.data.ecs_swift import ECSSwiftDataLayer
    from swiftclient.exceptions import ClientException
except ImportError:
    pytestmark = pytest.mark.skipif(True, reason='missing configuration file')


data = [('1', 'a'), ('2', 'b'), ('3', 'c'), ('4', 'd'), ('5', 'd')]


class TestECSSwift:

    def setup_method(self, test_method):
        self.ecs = ECSSwiftDataLayer(authurl, user, password,
                                     namespace, bucket_name)
        self.ecs._ecs_swift.put_container(bucket_name)

    def _put(self):
        for i in range(len(data)):
            self.ecs.put(data[i][0], data[i][1])

    def test_get_existing_keys(self):
        self._put()
        for i in range(len(data)):
            assert self.ecs.get(data[i][0]) == data[i][1]

    def test_get_missing_keys(self):
        self._put()
        keys = [10, 15]
        for k in keys:
            with pytest.raises(ClientException):
                self.ecs.get(k)

    def teardown_method(self, test_method):
        self.ecs.clean()
