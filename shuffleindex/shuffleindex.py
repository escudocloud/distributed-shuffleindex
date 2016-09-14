from operator import itemgetter
from bisect import bisect_right
from utils import chunks
from time import time
from six import iterkeys

class Node(object):

    __last_ID = 0

    def __init__(self):
        self.ID = self.PID = Node.__last_ID = Node.__last_ID + 1
        self.update_timestamp()

    @property
    def is_leaf(self):
        return isinstance(self, Leaf)

    def update_timestamp(self):
        self.ts = time()


class InnerNode(Node):

    def __init__(self, values, pointers):
        super(InnerNode, self).__init__()
        self._values, self._pointers = values, pointers

    @property
    def leftmost(self):
        return self._values[0]

    def child_to_follow(self, value):
        idx = bisect_right(self._values, value) - 1
        if idx < 0: raise KeyError(value)
        return self._pointers[idx]

    def __str__(self):
        return 'InnerNode(%d, %s)' % (self.ID, self._pointers)

    def __repr__(self):
        return self.__str__()


class Leaf(Node):

    def __init__(self, data):
        super(Leaf, self).__init__()
        self._data = data

    @property
    def leftmost(self):
        return min(iterkeys(self._data))

    def __getitem__(self, key):
        return self._data[key]

    def __str__(self):
        return 'Leaf(%d, %s)' % (self.ID, self._data)

    def __repr__(self):
        return self.__str__()


class Tree(object):

    def __init__(self, datalayer, fanout, leafsize):
        self._root = None
        self._nodes = datalayer
        self._fanout = fanout
        self._leafsize = leafsize

    def __getitem__(self, ID):
        return self._nodes.get(ID)

    def __setitem__(self, ID, node):
        return self._nodes.put(ID, node)

    def search(self, key):
        node = self._nodes.get(self._root)
        while not isinstance(node, Leaf):
            node = self._nodes.get(node.child_to_follow(key))
        return node[key]

    def bulk_load(self, data):
        data = sorted(data.items(), key=itemgetter(0)) # sort by key
        nodes = [Leaf(dict(chk)) for chk in chunks(data, self._leafsize)]
        while True:
            ptrs = [self._nodes.put(node.ID, node) for node in nodes]
            if len(ptrs) <= 1: break
            vals = [node.leftmost for node in nodes]
            chks = zip(chunks(vals, self._fanout), chunks(ptrs, self._fanout))
            nodes = [InnerNode(v, p) for (v, p) in chks]
        self._root = ptrs[0]

    def __str__(self):
        return self._nodes.__str__()
