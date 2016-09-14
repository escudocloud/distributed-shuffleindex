from shuffleindex.datalayer.memorylayer import *
from shuffleindex.datalayer.statslayer import *
from shuffleindex.shuffleindex import *
import pytest

class TestShuffleTree:

    def setup_method(self, test_method):
        self.datalayer = StatsLayer(MemoryDataLayer())
        self.tree = Tree(self.datalayer, fanout=2, leafsize=2)
        self.tree.bulk_load({1:'a', 2:'b', 3:'c', 4:'d', 5:'e'})

    def test_search_with_included_key(self):
        assert self.tree.search(5) == 'e'

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.tree.search(6)

    def test_statistics(self):
        n = 10                                              # number of accesses
        for _ in xrange(n): self.tree.search(1)           # access key 1 n times
        mc1, mc2 = self.datalayer.getcount.most_common(2)    # get most accessed
        assert mc1[1] == n                               # root accessed n times
        assert mc2[1] != n              # other nodes accessed less then n times
