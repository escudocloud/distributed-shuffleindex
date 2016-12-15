from memorylayer import MemoryDataLayer

try:
    from swift import SwiftDataLayer
except ImportError:
    print 'warning: cannot import swift library'

try:
    from gcs import GCSDataLayer
except ImportError:
    print 'warning: cannot import gcs library'

try:
    from s3 import S3DataLayer
except ImportError:
    print 'warning: cannot import s3 library'
