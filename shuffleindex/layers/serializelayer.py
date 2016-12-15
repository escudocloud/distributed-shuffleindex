from ..shuffleindex import Leaf, InnerNode
from collections import namedtuple
from datalayer import DataLayer
from array import array
import struct

_ENC = 'UTF-16LE'               # little endian (LE) to remove the BOM character
_HEAD, _HSIZE = '<QQQ?',  128           # pack header as ID, PID, elems, is_leaf
_LEAF, _LSIZE = '<Q120s', 128    # leaf block is uint64 as key and 120s as value
_NODE, _NSIZE = '<QQ',     16   # inner block is unit64 for both keys and values

assert struct.calcsize(_HEAD) <= _HSIZE         # check that header fits in size
assert struct.calcsize(_LEAF) == _LSIZE and struct.calcsize(_NODE) == _NSIZE

_HEAD = '%s%dx' % (_HEAD, _HSIZE - struct.calcsize(_HEAD))          # pad header
_Header = namedtuple('_Header', ['ID', 'PID', 'elems', 'is_leaf'])      # header


class SerializeLayer(DataLayer):

    def __init__(self, datalayer, datasize=4096):
        self._datalayer = datalayer
        self._datasize = datasize

    def get(self, key):
        buffer = array('c', self._datalayer.get(key))  # create buffer from data
        header = self._unpack_header(buffer, 0)      # unpack header at offset 0
        unpack = self._unpack_leaf if header.is_leaf else self._unpack_node
        node = unpack(header.elems, buffer, _HSIZE)    # offset unpack leaf/node
        node.ID, node.PID = header.ID, header.PID              # add header info
        return node

    def put(self, key, node):
        buffer = array('c', '\x00' * self._datasize)       # create empty buffer
        header = self._pack_header(node, buffer, 0)    # pack header at offset 0
        pack = self._pack_leaf if header.is_leaf else self._pack_node
        pack(node, buffer, _HSIZE)                        # offset pack leaf/node
        value = buffer.tostring()                     # convert buffer into bytes
        assert len(value) == self._datasize   # verify that the length is correct
        return self._datalayer.put(key, value)           # put to child datalayer


    def _pack_header(self, node, buffer, offset=0):
        elems = len(node._data if node.is_leaf else node._pointers) # get #elems
        header = _Header(node.ID, node.PID, elems, node.is_leaf) # create header
        struct.pack_into(_HEAD, buffer, offset, *list(header))     # pack header
        return header                  # return header object for other purposes

    def _pack_leaf(self, node, buffer, offset=_HSIZE):
        for key, value in node._data.items():
            struct.pack_into(_LEAF, buffer, offset, key, value.encode(_ENC))
            offset += _LSIZE

    def _pack_node(self, node, buffer, offset=_HSIZE):
        for value, pointer in zip(node._values, node._pointers):
            struct.pack_into(_NODE, buffer, offset, value, pointer)
            offset += _NSIZE

    def _unpack_header(self, buffer, offset=0):
        return _Header(*struct.unpack_from(_HEAD, buffer, offset)) # make header

    def _unpack_leaf(self, elems, buffer, base=_HSIZE):
        data = {}                                 # create empty data dictionary
        for offset in xrange(base, base + (_LSIZE * elems), _LSIZE): # for elems
            key, value = struct.unpack_from(_LEAF, buffer, offset)      # unpack
            data[key] = value.decode(_ENC).partition('\x00')[0]  # decode + trim
        return Leaf(data)                                   # create leaf object

    def _unpack_node(self, elems, buffer, base=_HSIZE):
        values, pointers = [], []       # create empty values and pointers lists
        for offset in xrange(base, base + (_NSIZE * elems), _NSIZE): # for elems
            value, pointer = struct.unpack_from(_NODE, buffer, offset)  # unpack
            values.append(value); pointers.append(pointer)     # append to lists
        return InnerNode(values, pointers)             # create InnerNode object
