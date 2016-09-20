from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.shufflelayer import *
from shuffleindex.layers.statslayer import *
from shuffleindex.shuffleindex import *
from testutils import gaussrange
from string import printable
import pytest

N = 1000                    # number of accesses to the datalayer in below tests

class TestShuffleTree:

    def setup_method(self, test_method):
        Node.set_last_id(0)                            # reset node counter to 0
        self.datalayer = StatsLayer(MemoryDataLayer()) # stats-memory-data-layer
        self.data = dict(enumerate(printable))                     # create data
        self.tree = Tree(self.datalayer, fanout=10, leafsize=2)    # create tree
        self.tree.bulk_load(self.data)            # bulk load data into the tree

    def test_search_with_included_key(self):
        assert self.tree.search(0) == printable[0]

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.tree.search(len(printable))

    def test_without_shuffle_index(self):
        for i in xrange(N):
            self.tree.search(gaussrange(len(self.data)))
        self.datalayer.plot_get()
        self.datalayer.plot_put()

    def test_with_shuffle_index(self):
        shufflelayer = ShuffleLayer(self.datalayer, num_cover=2, num_cache=1)
        shufflelayer.init_cache(self.tree._root)
        for i in xrange(N):
            shufflelayer.get(gaussrange(len(self.data)))
        self.datalayer.plot_get()
        self.datalayer.plot_put()
