from shuffleindex.datalayer.memorylayer import *
from shuffleindex.datalayer.statslayer import *
from shuffleindex.shuffleindex import *
from string import printable
import pytest

class TestShuffleTree:

    def setup_method(self, test_method):
        self.datalayer = StatsLayer(MemoryDataLayer())
        self.tree = Tree(self.datalayer, fanout=2, leafsize=2)
        self.tree.bulk_load(dict(enumerate(printable)))

    def test_search_with_included_key(self):
        assert self.tree.search(0) == printable[0]

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.tree.search(len(printable))

#    def test_statistics(self):
#        n = 10                                              # number of accesses
#        for _ in xrange(n): self.tree.search(1)           # access key 1 n times
#        mc1, mc2 = self.datalayer.getcount.most_common(2)    # get most accessed
#        assert mc1[1] == n                               # root accessed n times
#        assert mc2[1] != n              # other nodes accessed less then n times
