from six.moves import xrange
import struct

ENC = 'UTF-16LE' # we use little endian (LE) to remove the BOM character
FMT = '60s'     # format is a string of 60 characters

def encode(data, fmt=FMT, enc=ENC):
    """Encode data with specified format and encoding"""
    data = data.encode(enc)
    return struct.pack(fmt, data)

def decode(data, fmt=FMT, enc=ENC):
    """Decode data with specified format and encoding"""
    (data,) = struct.unpack(fmt, data)
    return data.decode(enc).partition('\0')[0]

def chunks(enum, n):
    """Yield successive n-sized chunks from enum"""
    for i in xrange(0, len(enum), n):
        yield enum[i:i+n]
