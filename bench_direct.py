from shuffleindex.layers.multidirectsearchlayer import *
from shuffleindex.layers.authencryptionlayer import *
from shuffleindex.layers.data.memorylayer import *
from shuffleindex.layers.serializelayer import *
from shuffleindex.layers.statslayer import *
from shuffleindex.layers.data.swift import *
from shuffleindex.layers.data.ecs_swift import *
from shuffleindex.layers.data.ecs_s3 import *
from shuffleindex.layers.data.s3 import *
from shuffleindex.multishuffleindex import *
from ConfigParser import ConfigParser
from argparse import ArgumentParser
from operator import attrgetter
from random import randrange
from string import printable
from time import time
from tqdm import tqdm
import numpy as np


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


def remote_datalayer(lowerlayer):
    authencryptionlayer = AuthEncryptionLayer(lowerlayer, key=enc_key)
    serializelayer = SerializeLayer(authencryptionlayer)
    statslayer = StatsLayer(serializelayer)                    # stats datalayer
    return statslayer


if __name__ == '__main__':
    parser = ArgumentParser(description='Test the Direct Search Layer')
    parser.add_argument('TEST', choices=['local', 'remote', 's3', 'ecs_s3', 'ecs_swift'])
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
    numdata = S * leafsize * (fanout ** levels)
    values  = (printable * (1 + numdata // len(printable)))[:numdata]
    data    = dict(enumerate(values))                             # create data
    assert len(data) == numdata

    if args.TEST == 'local':
        statslayers = [local_datalayer() for _ in xrange(S)]

    elif args.TEST == 'remote':
        config = ConfigParser()
        config.read(args.config)
        statslayers = [remote_datalayer(
            SwiftDataLayer(config.get('swift', 'authurl'),
                           config.get('swift', 'username'),
                           config.get('swift', 'key'),
                           config.get('swift', 'tenant_name'),
                           config.get('swift', 'container_name')))
                       for _ in xrange(S)]

    elif args.TEST == 's3':
        config = ConfigParser()
        config.read(args.config)
        statslayers = [remote_datalayer(
            S3DataLayer(config.get('s3', 'access_key'),
                        config.get('s3', 'secret_key'),
                        config.get('s3', 'bucket_name')))
                       for _ in xrange(S)]

    elif args.TEST == 'ecs_s3':
        config = ConfigParser()
        config.read(args.config)
        statslayers = [remote_datalayer(
            ECSS3DataLayer(config.get('ecs_s3', 'access_key'),
                           config.get('ecs_s3', 'secret_key'),
                           config.get('ecs_s3', 'bucket_name'),
                           config.get('ecs_s3', 'host'),
                           config.getint('ecs_s3', 'port'),
                           config.getboolean('ecs_s3', 'is_secure')))
                       for _ in xrange(S)]

    elif args.TEST == 'ecs_swift':
        config = ConfigParser()
        config.read(args.config)
        statslayers = [remote_datalayer(
            ECSSwiftDataLayer(config.get('ecs_swift', 'authurl'),
                              config.get('ecs_swift', 'username'),
                              config.get('ecs_swift', 'key'),
                              config.get('ecs_swift', 'namespace'),
                              config.get('ecs_swift', 'bucket_name'),
                              config.get('ecs_swift', 'auth_version')))
                       for _ in xrange(S)]

    print 'putting contents ...'
    datalayer = MultiDirectSearchLayer(statslayers)
    tree = MultiTree(datalayer, fanout=fanout, leafsize=leafsize)
    tree.bulk_load(data)                          # bulk load data into the tree
    datalayer.set_root_ids(tree._roots)                          # set the roots

    print 'getting contents ...'
    times = []

    for i in tqdm(xrange(N)):                                  # access the data
        start = time()
        key = randrange(numdata)
        datalayer.get(key)
        times.append(time() - start)

    print ''
    print 'avg access time: %.5f s' % np.mean(times)
    print 'std access time: %.5f s' % np.std(times)
