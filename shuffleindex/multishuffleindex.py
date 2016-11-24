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

    def __eq__(self, other):
        return (isinstance(other, Node) and
                self.ID == other.ID and self.PID == other.PID)

    @classmethod
    def set_last_id(cls, last_ID=0):
        cls.__last_ID = last_ID


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

    def __eq__(self, other):
        return (isinstance(other, InnerNode) and
                super(InnerNode, self).__eq__(other) and
                self._values == other._values and
                self._pointers == other._pointers)

    def __str__(self):
        return 'InnerNode(%d [was %d], %s)' % \
                (self.ID, self.PID, self._pointers)

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

    def __eq__(self, other):
        return (isinstance(other, Leaf) and
                super(Leaf, self).__eq__(other) and
                self._data == other._data)

    def __str__(self):
        return 'Leaf(%d [was %s], %s)' % (self.ID, self.PID, self._data)

    def __repr__(self):
        return self.__str__()


class MultiTree(object):

    def __init__(self, datalayer, fanout, leafsize):
        self._roots = None
        self._datalayer = datalayer
        self._fanout = fanout
        self._leafsize = leafsize

    def bulk_load(self, data):
        servers = self._datalayer._num_servers
        if self._fanout <= servers: raise RuntimeError('ERR: Fanout <= servers')
        data = sorted(data.items(), key=itemgetter(0))             # sort by key
        nodes = [Leaf(dict(chk)) for chk in chunks(data, self._leafsize)]
        while True:
            ptrs = [self._datalayer.put(node.ID, node) for node in nodes]
            print 'level with %d nodes' % len(ptrs)
            if len(ptrs) == servers: break                    # distributed root
            if len(ptrs) < servers: raise RuntimeError('too few values in root')
            vals = [node.leftmost for node in nodes]
            chks = zip(chunks(vals, self._fanout), chunks(ptrs, self._fanout))
            nodes = [InnerNode(v, p) for (v, p) in chks]

        self._roots = ptrs
