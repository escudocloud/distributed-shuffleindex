from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.predicatelayer import *
from shuffleindex.layers.serializelayer import *
from shuffleindex.layers.serializelayer import _HSIZE, _LSIZE, _NSIZE, _LEAF
from shuffleindex.shuffleindex import *
import struct
import pytest

# the maximum value length is (leaf size - uint64 [for key]) // 2 [for UTF-16]
VAL_LENGTH = (struct.calcsize(_LEAF) - struct.calcsize('<Q')) // 2

class TestSerializeLayer:

    def setup_method(self, test_method):
        self.memorylayer = MemoryDataLayer()
        self.predicatelayer = PredicateLayer(self.memorylayer)
        self.datasize = 4096
        self.datalayer = SerializeLayer(self.predicatelayer, self.datasize)

    def assert_serialize(self, key, value):
        assert not isinstance(value, str)     # assert that it is not serialized
        self.datalayer.put(key, value)                # put (serialize) the node
        predicate = lambda k, v: isinstance(v, str) and len(v) == self.datasize
        self.predicatelayer.put_predicate = predicate     # define put predicate
        received = self.datalayer.get(key)              # get (unserialize) node
        assert value == received               # assert attributes are preserved

    def test_serialized_leaf(self):
        self.assert_serialize(1, Leaf({0: '0'}))

    def test_serialized_innernode(self):
        self.assert_serialize(1, InnerNode([1], [2]))

    def test_full_leaf(self):
        elems = (self.datasize - _HSIZE) / _LSIZE       # max number of elements
        key, value = 1, Leaf({i:'x' for i in xrange(elems)})  # create full leaf
        self.assert_serialize(key, value)

    def test_full_innernode(self):
        elems = (self.datasize - _HSIZE) / _NSIZE       # max number of elements
        key, value = 1, InnerNode(range(elems), range(elems)) # create full node
        self.assert_serialize(key, value)

    def test_oversized_leaf(self):
        elems = (self.datasize - _HSIZE) / _LSIZE + 1 # max+1 number of elements
        key, value = 1, Leaf({i:'x' for i in xrange(elems)})  # create full leaf
        with pytest.raises(struct.error):
            self.assert_serialize(key, value)

    def test_oversized_innernode(self):
        elems = (self.datasize - _HSIZE) / _NSIZE + 1 # max+1 number of elements
        key, value = 1, InnerNode(range(elems), range(elems)) # create full node
        with pytest.raises(struct.error):
            self.assert_serialize(key, value)

    def test_max_length_string_in_leaf(self):
        key, value = 1, Leaf({0: 'x' * VAL_LENGTH})
        assert len(value[0]) == VAL_LENGTH
        self.datalayer.put(key, value)
        received = self.datalayer.get(key)
        assert len(received[0]) == VAL_LENGTH
        assert value[0] == received[0]

    def test_trimmed_string_in_leaf(self):
        key, value = 1, Leaf({0: 'x' * (VAL_LENGTH + 1)})
        assert len(value[0]) > VAL_LENGTH
        self.datalayer.put(key, value)
        received = self.datalayer.get(key)
        assert len(received[0]) == VAL_LENGTH
        assert value[0][:VAL_LENGTH] == received[0]
