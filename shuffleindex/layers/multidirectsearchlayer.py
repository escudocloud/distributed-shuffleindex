from datalayer import DataLayer
from operator import attrgetter
from bisect import bisect_right
from ..multishuffleindex import Leaf

class MultiDirectSearchLayer(DataLayer):

    def __init__(self, datalayers):
        self._num_servers = len(datalayers)
        self._datalayers = datalayers

    def set_root_ids(self, root_ids):
        self._root_ids = root_ids

    def _node_dl(self, ID):
        return self._datalayers[ID % self._num_servers]

    def get(self, key):
        roots = list(map(self._get, self._root_ids))
        roots.sort(key=attrgetter('leftmost'))
        lefts = [root.leftmost for root in roots]
        idx = bisect_right(lefts, key) - 1
        node = roots[idx]

        while not isinstance(node, Leaf):
            node = self._get(node.child_to_follow(key))
        return node[key]

    def put(self, key, value):
        return self._node_dl(key).put(key, value)

    def _get(self, key):
        return self._node_dl(key).get(key)
