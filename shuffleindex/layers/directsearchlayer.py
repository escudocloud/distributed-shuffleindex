from datalayer import DataLayer
from ..shuffleindex import Leaf

class DirectSearchLayer(DataLayer):

    def __init__(self, datalayer, root_id):
        self._datalayer = datalayer
        self._root_id = root_id

    def get(self, key):
        node = self._datalayer.get(self._root_id)
        while not isinstance(node, Leaf):
            node = self._datalayer.get(node.child_to_follow(key))
        return node[key]

    def put(self, key, value):
        return self._datalayer.put(key, value)
