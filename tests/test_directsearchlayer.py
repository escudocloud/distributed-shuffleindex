from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.directsearchlayer import *
from shuffleindex.layers.statslayer import *
from shuffleindex.shuffleindex import *
from utils.testutils import gaussrange
from string import printable
import pytest

# use TkAgg matplotlib backend which works also in virtual environments
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt

N = 1000                    # number of accesses to the datalayer in below tests

class TestDirectSearchLayer:

    def setup_method(self, test_method):
        Node.set_last_id(0)                            # reset node counter to 0
        self.memorylayer = MemoryDataLayer()                  # memory datalayer
        self.statslayer = StatsLayer(self.memorylayer)         # stats datalayer
        self.data = dict(enumerate(printable))                     # create data
        self.tree = Tree(self.statslayer, fanout=10, leafsize=2)   # create tree
        self.tree.bulk_load(self.data)            # bulk load data into the tree
        self.datalayer = DirectSearchLayer(self.statslayer, self.tree._root)

    def test_search_with_included_key(self):
        assert self.datalayer.get(0) == printable[0]

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.datalayer.get(len(printable))

    def test_without_shuffle_index(self):
        for i in xrange(N):
            self.datalayer.get(gaussrange(len(self.data)))

        self.statslayer.plot_get(show=False)
        plt.savefig('test_directsearchlayer_get.pdf')
        plt.clf()

        self.statslayer.plot_put(show=False)
        plt.savefig('test_directsearchlayer_put.pdf')
        plt.clf()
