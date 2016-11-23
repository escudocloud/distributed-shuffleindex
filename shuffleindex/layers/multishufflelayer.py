from datalayer import DataLayer
from collections import Counter, defaultdict
from itertools import chain, count, permutations
from operator import attrgetter, methodcaller
from random import choice, sample, shuffle
from bisect import bisect_right
from copy import copy
from six import itervalues
import logging

def _derangements(lst, fn=lambda x: x):
    shf = lst[:]           # make a copy of the list (shuffle operates in place)
    shuffle(shf)           # make a first shuffle to make permutations random
    for permutation in permutations(shf):
        if all(fn(x) != fn(y) for x, y in zip(lst, permutation)):
            yield permutation
    raise StopIteration('No other derangement found for %s' % lst)

class MultiShuffleLayer(DataLayer):

    def __init__(self, datalayers):
        self._num_servers = len(datalayers)
        self._datalayers = datalayers

    def set_root_ids(self, root_ids):
        self._root_ids = root_ids

    def _node_dl(self, ID):
        return self._datalayers[ID % self._num_servers]

    def put(self, key, value):
        return self._node_dl(key).put(key, value)

    def _get(self, key):
        return self._node_dl(key).get(key)

    @staticmethod
    def _update_node_ids(nodes, pi):
        previous = [nodes.pop(ID) for ID in list(pi.keys())]
        for node in previous:
            logging.debug('%d -> %d' % (node.ID, pi[node.ID]))
            node.ID = pi[node.ID]
            nodes[node.ID] = node

    def _group_by_dl(self, nodes):
        groups = defaultdict(list)
        for node in nodes:
            groups[self._node_dl(node)].append(node)
        return groups

    def get(self, target_value):
        assert len(self._datalayers) == len(self._root_ids)

        ids = self._root_ids
        parents = {ID: self._get(ID) for ID in ids}
        logging.debug('root nodes downloaded: %s' % parents)

        # create a derangement of parents (on different nodes)
        pi = dict(zip(ids, next(_derangements(ids, self._node_dl))))
        logging.debug('root derangement: %s' % pi)

        self._update_node_ids(parents, pi)
        logging.debug('root nodes after derangement: %s' % parents)

        nodes = sorted(parents.values(), key=attrgetter('leftmost'))
        lefts = [node.leftmost for node in nodes]
        idx = bisect_right(lefts, target_value) - 1
        parent = nodes[idx]                # the root node that has target_value
        logging.debug('parent_id: %s' % parent.ID)

        # search and shuffle
        for l in count(start=1):

            if parent.is_leaf: break
            target_id = parent.child_to_follow(target_value)  #target at level l
            logging.debug('target_id at level %d: %d' % (l, target_id))

            parent_nodes = parents.values()
            to_read, servers = [target_id], {self._node_dl(target_id)}
            children = [ID for p in parent_nodes for ID in p._pointers]
            trials = 1

            while True:             # derangements may fail due to bad selection
                logging.debug('derangement trial #%d' % trials)

                while len(to_read) != self._num_servers:
                    child = choice(children)
                    server = self._node_dl(child)
                    if server not in servers:
                        to_read.append(child)
                        servers.add(server)

                assert len(servers) == len(to_read) == self._num_servers
                logging.debug('to_read: %s' % to_read)

                # test if valid derangement
                for derangement in _derangements(to_read, self._node_dl):
                    pi = dict(zip(to_read, derangement))
                    logging.debug('derangement: %s' % pi)
                    #pi = dict(zip(range(244,253,3), range(254,262,3)))

                    new_pointers = []
                    for parent in parent_nodes:
                        new = [pi.get(x, x) for x in parent._pointers]
                        if len(self._group_by_dl(new)) != self._num_servers:
                            break
                        new_pointers.append(new)

                    if len(new_pointers) == len(parent_nodes):
                        break                          # valid derangement found

                else:
                    logging.debug('no valid derangement for this to_read selection')
                    trials += 1
                    continue

                break                                  # valid to_read selection

            read = {ID: self._get(ID) for ID in to_read}        # read the nodes
            self._update_node_ids(read, pi)        # swap nodes in read as in pi
            logging.debug(read)

            for parent, pointers in zip(parent_nodes, new_pointers):
                parent._pointers = pointers         # update pointers in parents
                logging.debug('writing node: %s' % parent.ID)
                self.put(parent.ID, parent)           # push parent to datalayer

            target_id = pi[target_id]                         # update target_id
            # it is not necessary to update to_read since they are recomputed

            parents = read
            parent = parents[target_id]

        for node in read.values():
            logging.debug('writing node: %s' % node.ID)
            self.put(node.ID, node)                     # push node to datalayer

        node = parents[target_id]                        # get correct leaf node
        assert node.is_leaf                              # check that it is leaf
        return node[target_value]                             # obtain the value
