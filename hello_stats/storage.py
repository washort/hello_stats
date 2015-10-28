"""Persistence of overnight data in S3"""

import cPickle as pickle
import json

from boto.s3.connection import S3Connection
from boto.s3.key import Key


class Bucket(object):
    """An abstraction to reading and writing to a single key in an S3 bucket"""

    def __init__(self, bucket_name, key, access_key_id, secret_access_key):
        s3 = S3Connection(aws_access_key_id=access_key_id,
                          aws_secret_access_key=secret_access_key)
        bucket = s3.get_bucket(bucket_name, validate=False)  # We lack the privs to validate, so the bucket had better exist.
        self._key = Key(bucket=bucket, name=key)

    def read(self):
        return self._read(self._key.get_contents_as_string())

    def write(self, data):
        self._key.set_contents_from_string(self._write(data))


class VersionedJsonBucket(Bucket):
    """An abstraction for reading and writing a JSON-formatted data structure
    to a single *existing* S3 key

    If we start to care about more keys, revise this to at least share
    a connection.

    """
    def _read(self, contents):
        """Return JSON-decoded contents of the S3 key and the version of its
        format."""
        # UTF-8 comes out of get_contents_as_string().
        # We expect the key to have something (like {}) in it in the first
        # place. I don't trust boto yet to not spuriously return ''.
        contents = json.loads(contents)
        return contents.get('version', 0), contents.get('metrics', [])

    def _write(self, data):
        """Save a JSON-encoded data structure to the S3 key."""
        # UTF-8 encoded:
        contents = {'version': VERSION, 'metrics': data}
        return json.dumps(contents, ensure_ascii=True, separators=(',', ':'))


class PickleBucket(Bucket):
    def _read(self, contents):
        return pickle.loads(contents)

    def _write(self, data):
        return pickle.dumps(data)
