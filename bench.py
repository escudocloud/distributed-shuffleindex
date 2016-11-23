from shuffleindex.layers.authencryptionlayer import *
from shuffleindex.layers.multishufflelayer import *
from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.serializelayer import *
from shuffleindex.layers.statslayer import *
from shuffleindex.layers.data.swift import *
from shuffleindex.multishuffleindex import *
from ConfigParser import ConfigParser
from argparse import ArgumentParser
from operator import attrgetter
from random import randrange
from string import printable
from time import time
from tqdm import tqdm


N        = 100              # number of accesses to the datalayer in below tests
S        = 3                                                 # number of servers
levels   = 1                                                  # number of levels
fanout   = S ** 3
leafsize = S ** 3
enc_key  = '0123456789ABCDEF'


def local_datalayer():
    memorylayer = MemoryDataLayer()                           # memory datalayer
    statslayer = StatsLayer(memorylayer)                       # stats datalayer
    return statslayer

def remote_datalayer(authurl, username, key, tenant_name, container_name):
    swiftlayer = SwiftDataLayer(authurl=authurl,
                                username=username,
                                key=key,
                                tenant_name=tenant_name,
                                container_name=container_name)
    authencryptionlayer = AuthEncryptionLayer(swiftlayer, key=enc_key)
    serializelayer = SerializeLayer(authencryptionlayer)
    statslayer = StatsLayer(serializelayer)                    # stats datalayer
    return statslayer


if __name__ == '__main__':
    parser = ArgumentParser(description='Test the Shuffle Index')
    parser.add_argument('TEST', choices=['local', 'remote'])
    parser.add_argument('--accesses', type=int, default=N)
    parser.add_argument('--servers', type=int, default=S)
    parser.add_argument('--levels', type=int, default=levels)
    parser.add_argument('--fanout', type=int, default=fanout)
    parser.add_argument('--leafsize', type=int, default=leafsize)
    parser.add_argument('--config', type=str, default='config.ini')
    args = parser.parse_args()

    N, S, levels, fanout, leafsize = attrgetter(
            'accesses', 'servers', 'levels', 'fanout', 'leafsize')(args)

    print 'creating data ...'
    numdata  = S * leafsize * (fanout ** levels)
    values   = (printable * (1 + numdata // len(printable)))[:numdata]
    data     = dict(enumerate(values))                             # create data
    assert len(data) == numdata

    if args.TEST == 'local':
        statslayers = [local_datalayer() for _ in xrange(S)]
    else:
        config = ConfigParser()
        config.read(args.config)
        create_datalayer = remote_datalayer
        statslayers = [remote_datalayer(config.get('swift', 'authurl'),
                                        config.get('swift', 'username'),
                                        config.get('swift', 'key'),
                                        config.get('swift', 'tenant_name'),
                                        config.get('swift', 'container_name'))
                       for _ in xrange(S)]

    print 'putting contents ...'
    shufflelayer = MultiShuffleLayer(statslayers)
    tree = MultiTree(shufflelayer, fanout=fanout, leafsize=leafsize)
    tree.bulk_load(data)                          # bulk load data into the tree
    shufflelayer.set_root_ids(tree._roots)                       # set the roots

    print 'getting contents ...'
    start = time()

    for i in tqdm(xrange(N)):                                  # access the data
        key = randrange(numdata)
        shufflelayer.get(key)

    elapsed = time() - start
    print ''
    print 'total access time: %.5f s' % elapsed
    print 'avg access time: %.5f s' % (elapsed / N)
