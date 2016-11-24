from shuffleindex.layers.multishufflelayer import *
from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.statslayer import *
from shuffleindex.multishuffleindex import *
from utils.testutils import gaussrange
from string import printable
import pytest

# use TkAgg matplotlib backend which works also in virtual environments
import matplotlib
matplotlib.use('TkAgg')
import matplotlib.pyplot as plt


N        = 1000             # number of accesses to the datalayer in below tests
S        = 3                                                 # number of servers
levels   = 2                                                  # number of levels
fanout   = S ** 3
leafsize = S ** 3
numdata  = S * leafsize * (fanout ** levels)
values   = (printable * (1 + numdata // len(printable)))[:numdata]
data     = dict(enumerate(values))                                 # create data
assert len(data) == numdata


class TestShuffleLayer:

    @staticmethod
    def _create_datalayer():
        memorylayer = MemoryDataLayer()                       # memory datalayer
        statslayer = StatsLayer(memorylayer)                   # stats datalayer
        return statslayer

    def setup_method(self, test_method):
        Node.set_last_id(0)                            # reset node counter to 0
        self.statslayer = [self._create_datalayer() for _ in xrange(S)]
        self.datalayer = MultiShuffleLayer(self.statslayer)
        self.tree = MultiTree(self.datalayer, fanout=fanout, leafsize=leafsize)
        self.tree.bulk_load(data)                 # bulk load data into the tree
        self.datalayer.set_root_ids(self.tree._roots)            # set the roots

    def test_search_with_included_key(self):
        for key in xrange(min(N, len(data))):
            assert self.datalayer.get(key) == data[key]

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.datalayer.get(len(data))

    def test_with_shuffle_index(self):
        for statslayer in self.statslayer:
            statslayer.reset()

        for i in xrange(N):
            self.datalayer.get(0)                          # worst case scenario

        for row, statslayer in enumerate(self.statslayer):
            plt.subplot(len(self.statslayer), 2, 1 + 2 * row)
            plt.yscale('log')
            statslayer.plot_get(show=False)
            plt.subplot(len(self.statslayer), 2, 2 + 2 * row)
            plt.yscale('log')
            statslayer.plot_put(show=False)

        plt.tight_layout()
        plt.savefig('test_multishufflelayer.pdf')
        plt.clf()
