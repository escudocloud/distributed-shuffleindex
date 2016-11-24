from six.moves import xrange, cPickle as pickle
from datalayer import DataLayer
from itertools import chain, count
from operator import attrgetter
from random import choice, sample, shuffle
from six import itervalues
import logging


class ShuffleLayer(DataLayer):

    def __init__(self, datalayer, num_cover, num_cache):
        self._datalayer = datalayer
        self._num_cover = num_cover
        self._num_cache = num_cache
        self._cache = dict()

    def init_cache(self, root_id):
        root_node = self._datalayer.get(root_id)
        root_node.update_timestamp()
        self._cache[0] = {root_id: root_node}
        ids = sample(root_node._pointers, self._num_cache)

        for level in count(start=1):
            nodes = map(self._datalayer.get, ids)
            for node in nodes: node.update_timestamp()
            self._cache[level] = dict(zip(ids, nodes))
            if any(node.is_leaf for node in nodes): break
            ids = [choice(node._pointers) for node in nodes]

    def save_cache(self, filename):
        with open(filename, 'w') as fp:
            pickle.dump(self._cache, fp)

    def load_cache(self, filename):
        with open(filename) as fp:
            self._cache = pickle.load(fp)

    def put(self, key, value):
        return self._datalayer.put(key, value)

    def get(self, target_value):

        def update_node_ids(nodes, pi):
            previous = [nodes.pop(ID) for ID in list(nodes.keys())]
            for node in previous:
                logging.debug('%d -> %d' % (node.ID, pi[node.ID]))
                node.ID = pi[node.ID]
                nodes[node.ID] = node

        non_cached, non_cached_p = dict(), dict()
        n0 = self._cache[0].values()[0]    # let n0 be the unique node in Cache0
        target_id = n0.ID                          # start search from root node
        cache_hit = True                     # the root always belongs to Cache0
        num_cover = self._num_cover + 1     # + 1 to leave space for target node

        # generate cover_id searches from root pointers that are not in cache
        cover_id = list(sample(set(n0._pointers) - set(self._cache[1].keys())
                          - set([n0.child_to_follow(target_value)]), num_cover))

        # search, shuffle, and update cache and index structure
        for l in count(start=1):
            n = self._cache[l-1][target_id]       # get previous node on path
            if n.is_leaf: break                   # if leaf it is the target
            target_id = n.child_to_follow(target_value) # otherwise get child
            logging.debug('target_id: %s' % target_id)

            # identify the blocks to read from the server
            if target_id not in self._cache[l]:   # target_id node not cached
                to_read_ids = [target_id]         # we need to read it
                if cache_hit:                     # if it's first non-cached ..
                    cache_hit = False             # .. mark as out of cache ..
                    num_cover -= 1                # .. and drop one cover search
                    del cover_id[-1]              # .. and drop last cover_id
            else:
                logging.debug('target in cache')
                to_read_ids = []     # we do not need to read target_id (cached)

            if l != 1:               # cover_id for level 1 are already computed
                for i, ID in enumerate(list(cover_id)):  # for every cover_id ..
                    n = self._cache[l-1].get(ID, None) or non_cached_p[ID]
                    cover_id[i] = choice(n._pointers)   # .. follow random child

            to_read_ids.extend(cover_id)                 # extend it with covers
            shuffle(to_read_ids)      # shuffle them to not have target_id first
            assert len(to_read_ids) == self._num_cover + 1

            # read blocks
            logging.debug('reading blocks: %s' % to_read_ids)
            read = dict(zip(to_read_ids, map(self._datalayer.get, to_read_ids)))

            # shuffle nodes
            ids = list(to_read_ids) + list(self._cache[l].keys())      # get ids
            pi = dict(zip(ids, sample(ids, len(ids))))      # random permutation
            logging.debug('pi: %s' % pi)

            # update ids on read and cache[l]. They'll be put at next iteration
            update_node_ids(read, pi)               # update node ids in read ..
            update_node_ids(self._cache[l], pi)     # .. and in cache[l]

            # determine effects on parents and store nodes at level l-1
            for node in chain(self._cache[l-1].values(), non_cached_p.values()):
                # use change ID with pi[ID] or fallback again to ID if no pi[ID]
                logging.debug('node %d pointers %s' % (node.ID, node._pointers))
                node._pointers = [pi.get(ID, ID) for ID in node._pointers]
                logging.debug('-> %s' % node._pointers)
                logging.debug('writing node %d' % node.ID)
                self._datalayer.put(node.ID, node)                # writing node

            logging.debug('target_id %d -> %d' % (target_id, pi[target_id]))
            target_id = pi[target_id]                 # update also target_id ..
            logging.debug('cover_id %s ->' % cover_id)
            cover_id = [pi.get(ID, ID) for ID in cover_id]    # .. and cover_ids
            logging.debug('-> %s' % cover_id)

            # update cache at level l
            non_cached = read

            if cache_hit:                    # if cache_hit update the timestamp
                self._cache[l][target_id].update_timestamp()
                logging.debug('timestamp updated')
            else:                          # otherwise put target_id in cache[L]
                deleted = min(itervalues(self._cache[l]), key=attrgetter('ts'))
                logging.debug('removing %d from cache[%d]' % (deleted.ID, l))
                del self._cache[l][deleted.ID]        # remove LRU from cache[L]
                n = read[target_id]                   # get last node on path ..
                logging.debug('add %d to cache[%d]' % (target_id, l))
                self._cache[l][target_id] = n         # .. to add it to cache[L]
                non_cached[deleted.ID] = deleted      # .. deleted in non_cached
                del non_cached[target_id]             # and remove the target_id

            non_cached_p = non_cached       # at last prepare for next iteration

        # write nodes at level h
        logging.debug(self._cache)
        for node in chain(self._cache[l-1].values(), non_cached_p.values()):
            logging.debug('writing node %d' % node.ID)
            self._datalayer.put(node.ID, node)                    # writing node

        n = self._cache[l-1][target_id]
        logging.debug(n)
        assert n.is_leaf
        return n[target_value]
