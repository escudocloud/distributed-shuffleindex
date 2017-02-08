from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.predicatelayer import *
import pytest

class TestPredicateLayer:

    def setup_method(self, test_method):
        self.memorylayer = MemoryDataLayer()
        self.datalayer = PredicateLayer(self.memorylayer)

    def test_predicates(self):
        right_predicate = lambda k, v: k == key and v == value
        wrong_predicate = lambda k, v: False
        key, value = 1, 'Hello, World!'

        with pytest.raises(PredicateException):
            self.datalayer.put_predicate = wrong_predicate
            self.datalayer.put(key, value)

        self.datalayer.put_predicate = right_predicate
        self.datalayer.put(key, value)

        with pytest.raises(PredicateException):
            self.datalayer.get_predicate = wrong_predicate
            self.datalayer.get(key)

        self.datalayer.get_predicate = right_predicate
        self.datalayer.get(key)
