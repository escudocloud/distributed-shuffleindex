from shuffleindex.layers.multidirectsearchlayer import *
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


class TestMultiDirectSearchLayer:

    @staticmethod
    def _create_datalayer():
        memorylayer = MemoryDataLayer()                       # memory datalayer
        statslayer = StatsLayer(memorylayer)                   # stats datalayer
        return statslayer

    def setup_method(self, test_method):
        Node.set_last_id(0)                            # reset node counter to 0
        self.statslayers = [self._create_datalayer() for _ in xrange(S)]
        self.datalayer = MultiDirectSearchLayer(self.statslayers)
        self.tree = MultiTree(self.datalayer, fanout=fanout, leafsize=leafsize)
        self.tree.bulk_load(data)                 # bulk load data into the tree
        self.datalayer.set_root_ids(self.tree._roots)            # set the roots

    def test_search_with_included_key(self):
        assert self.datalayer.get(0) == printable[0]

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.datalayer.get(numdata)

    def test_direct_gaussian(self, N):
        for i in xrange(N): self.datalayer.get(gaussrange(numdata))
        self._plot_results('tests/figure_multidirectsearch_gaussian', N)

    def test_direct_worst(self, N):
        for i in xrange(N): self.datalayer.get(numdata // 2)
        self._plot_results('tests/figure_multidirectsearch_worst', N)

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
