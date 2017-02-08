from s3 import S3DataLayer
from boto.s3.connection import S3Connection


class ECSS3DataLayer(S3DataLayer):

    def __init__(self, access_key, secret_key, bucket_name, host,
                 port=9020, is_secure=False, **kwargs):

        # create ECS customized connection
        self._connection = S3Connection(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key, host=host, port=port,
            is_secure=is_secure,
            calling_format='boto.s3.connection.ProtocolIndependentOrdinaryCallingFormat',
            **kwargs)

        # create bucket object
        self._bucket = self._connection.get_bucket(bucket_name)
