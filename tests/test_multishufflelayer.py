from shuffleindex.layers.multishufflelayer import *
from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.statslayer import *
from shuffleindex.multishuffleindex import *
from utils.testutils import gaussrange
from string import printable
import pytest

# use TkAgg matplotlib backend which works also in virtual environments
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt


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
        self.statslayers = [self._create_datalayer() for _ in xrange(S)]
        self.datalayer = MultiShuffleLayer(self.statslayers)
        self.tree = MultiTree(self.datalayer, fanout=fanout, leafsize=leafsize)
        self.tree.bulk_load(data)                 # bulk load data into the tree
        self.datalayer.set_root_ids(self.tree._roots)            # set the roots

    def test_search_with_included_key(self, N):
        for key in xrange(min(N, len(data))):
            assert self.datalayer.get(key) == data[key]

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.datalayer.get(len(data))

    def test_with_shuffle_index_gaussian(self, N):
        for statslayer in self.statslayers: statslayer.reset()
        for i in xrange(N): self.datalayer.get(gaussrange(numdata))
        self._plot_results('tests/figure_multishufflelayer_gaussian', N)

    def test_with_shuffle_index_worst(self, N):
        for statslayer in self.statslayers: statslayer.reset()
        for i in xrange(N): self.datalayer.get(0)
        self._plot_results('tests/figure_multishufflelayer_worst', N)

    def _plot_results(self, pathprefix, N):
        plt.figure(figsize=(4,6))
        for row, statslayer in enumerate(self.statslayers, start=1):
            plt.subplot(len(self.statslayers), 1, row)
            plt.yscale('log')
            plt.ylim(1, N)
            plt.gca().yaxis.tick_left()
            statslayer.plot_get(show=False, title='read (server %d)' % row)

        plt.tight_layout()
        plt.savefig(pathprefix + '_get.pdf')
        plt.clf()

        plt.figure(figsize=(4,6))
        for row, statslayer in enumerate(self.statslayers, start=1):
            plt.subplot(len(self.statslayers), 1, row)
            plt.yscale('log')
            plt.ylim(1, N)
            plt.gca().yaxis.tick_left()
            statslayer.plot_put(show=False, title='write (server %d)' % row)

        plt.tight_layout()
        plt.savefig(pathprefix + '_put.pdf')
        plt.clf()
