from shuffleindex.layers.data.gcs import GCSDataLayer
import pytest

try:
    from configs.gcs import *
except ImportError:
    pytestmark = pytest.mark.skipif(True, reason='missing configuration file')


data = [('1', 'a'), ('2', 'b'), ('3', 'c'), ('4', 'd'), ('5', 'd')]


class TestGCS:

    def setup_method(self, test_method):
        self.gcs = GCSDataLayer('configs/gcs_key.json', project, bucket)

    def _put(self):
        for i in range(len(data)):
            self.gcs.put(data[i][0], data[i][1])

    def test_get_existing_keys(self):
        self._put()
        for i in range(len(data)):
            assert self.gcs.get(data[i][0]) == data[i][1]

    def test_get_missing_keys(self):
        self._put()
        keys = [10, 15]
        for k in keys:
            with pytest.raises(AttributeError):
                self.gcs.get(k)

    def teardown_method(self, test_method):
        self.gcs.clean()
