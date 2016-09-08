from shuffleindex.datalayer.datalayer import *
from shuffleindex.shuffleindex import *
import pytest

class TestBPTree:

    def setup_method(self, test_method):
        datalayer = MemoryDataLayer()
        self.tree = Tree(datalayer, fanout=2, leafsize=2)
        self.tree.bulk_load({1:'a', 2:'b', 3:'c', 4:'d', 5:'e'})

    def test_search_with_included_key(self):
        assert self.tree.search(5) == 'e'

    def test_search_with_not_included_key(self):
        with pytest.raises(KeyError):
            self.tree.search(6)
