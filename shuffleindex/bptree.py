from bisect import bisect_right
from utils import chunks
from six import iterkeys

class Node(object):
    __last_ID = 0

    def __init__(self):
        self.ID = Node.__last_ID = Node.__last_ID + 1

    @property
    def is_leaf(self):
        return isinstance(self, Leaf)


class InnerNode(Node):
    def __init__(self, children):
        super(InnerNode, self).__init__()
        self._children = children
        self._values = [child.leftmost for child in self._children[1:]]

    @property
    def leftmost(self):
        return self._children[0].leftmost

    def child_to_follow(self, value):
        idx = bisect_right(self._values, value)
        return self._children[idx]

    def __getitem__(self, key):
        return self.child_to_follow(key)[key]

    def __str__(self):
        return 'InnerNode(%s)' % ','.join(str(n.ID) for n in self._children)

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
        return 'Leaf(%s)' % self._data

    def __repr__(self):
        return self.__str__()


class Tree(object):
    def __init__(self, fanout, leafsize):
        self._root = None
        self._nodes = dict()
        self._fanout = fanout
        self._leafsize = leafsize

    def __getitem__(self, ID):
        return self._nodes.__getitem__(ID)

    def __setitem__(self, ID, node):
        return self._nodes.__setitem__(ID, node)

    def add_nodes(self, nodes):
        self._nodes.update((node.ID, node) for node in nodes)

    def search(self, key):
        return self._root[key]

    def bulk_load(self, data):
        data = sorted(data.items(), key=lambda (k,v): k)
        nodes = list(map(Leaf, map(dict, chunks(data, self._leafsize))))
        self.add_nodes(nodes)
        while len(nodes) > 1:
            nodes = list(map(InnerNode, chunks(nodes, self._fanout)))
            self.add_nodes(nodes)
        self._root = nodes[0]

    def __str__(self):
        return self._nodes.__str__()
