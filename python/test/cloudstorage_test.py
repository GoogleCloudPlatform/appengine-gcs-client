# Copyright 2012 Google Inc. All Rights Reserved.

"""Tests for cloudstorage_api.py."""

from __future__ import with_statement



import gzip
import hashlib
import math
import os
import pickle
import time
import unittest

from google.appengine.ext import testbed


try:
  import cloudstorage
  from cloudstorage import cloudstorage_api
  from google.appengine.ext.cloudstorage import cloudstorage_stub
  from cloudstorage import common
  from cloudstorage import errors
  from cloudstorage import storage_api
except ImportError:
  from google.appengine.ext import cloudstorage
  from google.appengine.ext.cloudstorage import cloudstorage_api
  from google.appengine.ext.cloudstorage import cloudstorage_stub
  from google.appengine.ext.cloudstorage import common
  from google.appengine.ext.cloudstorage import errors
  from google.appengine.ext.cloudstorage import storage_api


BUCKET = '/bucket'
TESTFILE = BUCKET + '/testfile'
DESTFILE = BUCKET + '/destfile'
DEFAULT_COMPOSE_CONTENT = 'A'
DEFAULT_CONTENT = ['a'*1024*257,
                   'b'*1024*257,
                   'c'*1024*257]


class IrregularPatternTest(unittest.TestCase):
  """Invoke APIs in some unusual pattern.

  Mostly to test behaviors replied by MapReduce.
  """

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()
    self._old_max_keys = common._MAX_GET_BUCKET_RESULT
    common._MAX_GET_BUCKET_RESULT = 2
    self.start_time = time.time()
    cloudstorage.set_default_retry_params(None)

  def tearDown(self):
    common._MAX_GET_BUCKET_RESULT = self._old_max_keys
    self.testbed.deactivate()

  def testNoEffectAfterClose(self):
    """Test file ops after close are discarded."""
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('a'*(256+50)*1024)
    f2 = pickle.loads(pickle.dumps(f))
    f.write('b'*(50)*1024)
    f.close()

    self.assertRaises(IOError, f.write, 'foo')
    f.close()

    self.assertRaises(errors.FileClosedError, f2.write,
                      'c'*storage_api.StreamingBuffer._flushsize)

    a, b = 0, 0
    f = cloudstorage.open(TESTFILE)
    for c in f.read():
      if c == 'a':
        a += 1
      elif c == 'b':
        b += 1
    self.assertEqual(256+50, a/1024.0)
    self.assertEqual(50, b/1024.0)

  def testReuploadSameContent(self):
    """Test re write same content to same offset works."""
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('a'*(256+50)*1024)
    f2 = pickle.loads(pickle.dumps(f))
    f.write('b'*(256+50)*1024)
    f2.write('b'*(256+256+50)*1024)
    f2.close()
    a, b = 0, 0

    f = cloudstorage.open(TESTFILE)
    for c in f.read():
      if c == 'a':
        a += 1
      elif c == 'b':
        b += 1
    self.assertEqual(256+50, a/1024.0)
    self.assertEqual(256+256+50, b/1024.0)


class CloudStorageTest(unittest.TestCase):
  """Test for cloudstorage."""

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()
    self._old_max_keys = common._MAX_GET_BUCKET_RESULT
    common._MAX_GET_BUCKET_RESULT = 2
    self.start_time = time.time()
    cloudstorage.set_default_retry_params(None)

  def tearDown(self):
    common._MAX_GET_BUCKET_RESULT = self._old_max_keys
    self.testbed.deactivate()

  def CreateFile(self, filename):
    f = cloudstorage.open(filename,
                          'w',
                          'text/plain',
                          {'x-goog-meta-foo': 'foo',
                           'x-goog-meta-bar': 'bar',
                           'x-goog-acl': 'public-read',
                           'cache-control': 'public, max-age=6000',
                           'content-disposition': 'attachment; filename=f.txt'})
    for content in DEFAULT_CONTENT:
      f.write(content)
    f.close()

  def testFilenameEscaping(self):
    name = BUCKET + '/a b/c d/*%$'
    with cloudstorage.open(name, 'w') as f:
      f.write('foo')
    with cloudstorage.open(name) as f:
      self.assertEqual('foo', f.read())
    self.assertEqual(name, cloudstorage.stat(name).filename)
    bucket = cloudstorage.listbucket(BUCKET)
    for stat in bucket:
      self.assertEqual(name, stat.filename)
    cloudstorage.delete(name)

  def testGzip(self):
    with cloudstorage.open(TESTFILE, 'w', 'text/plain',
                           {'content-encoding': 'gzip'}) as f:
      gz = gzip.GzipFile('', 'wb', 9, f)
      gz.write('a'*1024)
      gz.write('b'*1024)
      gz.close()

    stat = cloudstorage.stat(TESTFILE)
    self.assertEqual('text/plain', stat.content_type)
    self.assertEqual('gzip', stat.metadata['content-encoding'])
    self.assertEqual(37, stat.st_size)

    with cloudstorage.open(TESTFILE) as f:
      gz = gzip.GzipFile('', 'rb', 9, f)
      result = gz.read(10)
      self.assertEqual('a'*10, result)
      self.assertEqual('a'*1014 + 'b'*1024, gz.read())

  def testFlush(self):
    blocksize = 0
    with cloudstorage.open(TESTFILE, 'w') as f:
      blocksize = f._blocksize
      f.write('a'*(blocksize-2))
      f.write('a'*3)
      f.write('a')
      f.flush()
      self.assertEqual(2, f._buffered)
      f.flush()
      f.write('a')
      f.close()

    with cloudstorage.open(TESTFILE) as f:
      self.assertEqual(blocksize + 3, len(f.read()))

  def testFlush2(self):
    blocksize = 0
    with cloudstorage.open(TESTFILE, 'w') as f:
      blocksize = f._blocksize
      f.write('a'*(blocksize+1))
      f.write('a')
      f.write('a'*(blocksize-1))
      f.flush()
      self.assertEqual(1, f._buffered)

    with cloudstorage.open(TESTFILE) as f:
      self.assertEqual(blocksize*2+1, len(f.read()))

  def testCopy2(self):
    with cloudstorage.open(TESTFILE, 'w',
                           'text/foo', {'x-goog-meta-foo': 'foo'}) as f:
      f.write('abcde')

    dst = TESTFILE + 'copy'
    self.assertRaises(cloudstorage.NotFoundError, cloudstorage.stat, dst)
    cloudstorage_api.copy2(TESTFILE, dst)

    src_stat = cloudstorage.stat(TESTFILE)
    dst_stat = cloudstorage.stat(dst)
    self.assertEqual(src_stat.st_ctime, dst_stat.st_ctime)
    self.assertEqual(src_stat.st_size, dst_stat.st_size)
    self.assertEqual(src_stat.etag, dst_stat.etag)
    self.assertEqual(src_stat.content_type, dst_stat.content_type)
    self.assertEqual(src_stat.metadata, dst_stat.metadata)

    with cloudstorage.open(dst) as f:
      self.assertEqual('abcde', f.read())

  def testCopy2ReplacesMetadata(self):
    with cloudstorage.open(TESTFILE, 'w',
                           'text/foo', {'x-goog-meta-foo': 'foo'}) as f:
      f.write('abcde')
    src_stat = cloudstorage.stat(TESTFILE)

    cloudstorage_api.copy2(TESTFILE, TESTFILE,
                           metadata={'x-goog-meta-foo': 'bar',
                                     'content-type': 'text/bar'})

    dst_stat = cloudstorage.stat(TESTFILE)
    self.assertEqual(src_stat.st_size, dst_stat.st_size)
    self.assertEqual(src_stat.etag, dst_stat.etag)
    self.assertEqual(src_stat.st_ctime, dst_stat.st_ctime)
    self.assertEqual('text/foo', src_stat.content_type)
    self.assertEqual('text/bar', dst_stat.content_type)
    self.assertEqual('foo', src_stat.metadata['x-goog-meta-foo'])
    self.assertEqual('bar', dst_stat.metadata['x-goog-meta-foo'])

    with cloudstorage.open(TESTFILE) as f:
      self.assertEqual('abcde', f.read())

  def testDelete(self):
    self.assertRaises(errors.NotFoundError, cloudstorage.delete, TESTFILE)
    self.CreateFile(TESTFILE)
    cloudstorage.delete(TESTFILE)
    self.assertRaises(errors.NotFoundError, cloudstorage.delete, TESTFILE)
    self.assertRaises(errors.NotFoundError, cloudstorage.stat, TESTFILE)

  def testGetLocation(self):
    self.assertEquals('US', cloudstorage.get_location('/some-bucket'))

    self.assertRaises(ValueError, cloudstorage.get_location, 'bad-format')
    self.assertRaises(ValueError, cloudstorage.get_location, '/bad-format/obj')

  def testGetStorageClass(self):
    self.assertEquals('STANDARD',
                      cloudstorage.get_storage_class('/some-bucket'))

    self.assertRaises(ValueError, cloudstorage.get_storage_class, 'bad-format')
    self.assertRaises(
        ValueError, cloudstorage.get_storage_class, '/bad-format/obj')

  def testReadEntireFile(self):
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('abcde')
    f.close()

    f = cloudstorage.open(TESTFILE, read_buffer_size=1)
    self.assertEqual('abcde', f.read())
    f.close()

    f = cloudstorage.open(TESTFILE)
    self.assertEqual('abcde', f.read(8))
    f.close()

  def testReadNonexistFile(self):
    self.assertRaises(errors.NotFoundError, cloudstorage.open, TESTFILE)

  def testRetryParams(self):
    retry_params = cloudstorage.RetryParams(max_retries=0)
    cloudstorage.set_default_retry_params(retry_params)

    retry_params.max_retries = 1000
    with cloudstorage.open(TESTFILE, 'w') as f:
      self.assertEqual(0, f._api.retry_params.max_retries)

    with cloudstorage.open(TESTFILE, 'w') as f:
      cloudstorage.set_default_retry_params(retry_params)
      self.assertEqual(0, f._api.retry_params.max_retries)

    per_call_retry_params = cloudstorage.RetryParams()
    with cloudstorage.open(TESTFILE, 'w',
                           retry_params=per_call_retry_params) as f:
      self.assertEqual(per_call_retry_params, f._api.retry_params)

  def testReadEmptyFile(self):
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('')
    f.close()

    f = cloudstorage.open(TESTFILE)
    self.assertEqual('', f.read())
    self.assertEqual('', f.read())
    f.close()

  def testReadSmall(self):
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('abcdefghij')
    f.close()

    f = cloudstorage.open(TESTFILE, read_buffer_size=3)
    self.assertEqual('ab', f.read(2))
    self.assertEqual('c', f.read(1))
    self.assertEqual('de', f.read(2))
    self.assertEqual('fghij', f.read())
    f.close()

  def testReadIterator(self):
    content = 'ab\n\ncd\nef\ng'
    with cloudstorage.open(TESTFILE, 'w') as f:
      f.write(content)

    f = cloudstorage.open(TESTFILE)
    lines = [line for line in f]
    self.assertEqual(content, ''.join(lines))

    lines = [line for line in f]
    self.assertEqual([], lines)

    f.seek(0)
    lines = [line for line in f]
    self.assertEqual(content, ''.join(lines))

    with cloudstorage.open(TESTFILE) as f:
      lines = [line for line in f]
      self.assertEqual(content, ''.join(lines))

  def testWriteRead(self):
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('a')
    f.write('b'*1024)
    f.write('c'*1024 + '\n')
    f.write('d'*1024*1024)
    f.write('e'*1024*1024*10)
    self.assertRaises(errors.NotFoundError, cloudstorage.stat, TESTFILE)
    f.close()

    f = cloudstorage.open(TESTFILE)
    self.assertEqual('a' + 'b'*1024, f.read(1025))
    self.assertEqual('c'*1024 + '\n', f.readline())
    self.assertEqual('d'*1024*1024, f.read(1024*1024))
    self.assertEqual('e'*1024*1024*10, f.read())
    self.assertEqual('', f.read())
    self.assertEqual('', f.readline())

  def WriteInBlockSizeTest(self):
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('a'*256*1024)
    f.write('b'*256*1024)
    f.close()

    f = cloudstorage.open(TESTFILE)
    self.assertEqual('a'*256*1024 + 'b'*256*1024, f.read())
    self.assertEqual('', f.read())
    self.assertEqual('', f.readline())
    f.close()

  def testWriteReadWithContextManager(self):
    with cloudstorage.open(TESTFILE, 'w') as f:
      f.write('a')
      f.write('b'*1024)
      f.write('c'*1024 + '\n')
      f.write('d'*1024*1024)
      f.write('e'*1024*1024*10)
    self.assertTrue(f.closed)

    with cloudstorage.open(TESTFILE) as f:
      self.assertEqual('a' + 'b'*1024, f.read(1025))
      self.assertEqual('c'*1024 + '\n', f.readline())
      self.assertEqual('d'*1024*1024, f.read(1024*1024))
      self.assertEqual('e'*1024*1024*10, f.read())
      self.assertEqual('', f.read())
      self.assertEqual('', f.readline())
    self.assertTrue(f.closed)

  def testSeekAndTell(self):
    f = cloudstorage.open(TESTFILE, 'w')
    f.write('abcdefghij')
    f.close()

    f = cloudstorage.open(TESTFILE)
    f.seek(5)
    self.assertEqual(5, f.tell())
    self.assertEqual('f', f.read(1))
    self.assertEqual(6, f.tell())
    f.seek(-1, os.SEEK_CUR)
    self.assertEqual('f', f.read(1))
    f.seek(-1, os.SEEK_END)
    self.assertEqual('j', f.read(1))

  def testStat(self):
    self.CreateFile(TESTFILE)
    filestat = cloudstorage.stat(TESTFILE)
    content = ''.join(DEFAULT_CONTENT)
    self.assertEqual(len(content), filestat.st_size)
    self.assertEqual('text/plain', filestat.content_type)
    self.assertEqual('foo', filestat.metadata['x-goog-meta-foo'])
    self.assertEqual('bar', filestat.metadata['x-goog-meta-bar'])
    self.assertEqual('public, max-age=6000', filestat.metadata['cache-control'])
    self.assertEqual(
        'attachment; filename=f.txt',
        filestat.metadata['content-disposition'])
    self.assertEqual(TESTFILE, filestat.filename)
    self.assertEqual(hashlib.md5(content).hexdigest(), filestat.etag)
    self.assertTrue(math.floor(self.start_time) <= filestat.st_ctime)
    self.assertTrue(filestat.st_ctime <= time.time())

  def testDefaultContentType(self):
    with cloudstorage.open(TESTFILE, 'w') as f:
      f.write('foo')
    filestat = cloudstorage.stat(TESTFILE)
    self.assertEqual(cloudstorage_stub._GCS_DEFAULT_CONTENT_TYPE,
                     filestat.content_type)

  def testListBucketCompatibility(self):
    """Test listbucket's old interface still works."""
    bars = [BUCKET + '/test/bar' + str(i) for i in range(3)]
    foos = [BUCKET + '/test/foo' + str(i) for i in range(3)]
    filenames = bars + foos
    for filename in filenames:
      self.CreateFile(filename)

    bucket = cloudstorage.listbucket(BUCKET, prefix='test/', marker='test/foo')
    self.assertEqual(foos, [stat.filename for stat in bucket])

  def testListBucket(self):
    bars = [BUCKET + '/test/bar' + str(i) for i in range(3)]
    foos = [BUCKET + '/test/foo' + str(i) for i in range(3)]
    filenames = bars + foos
    for filename in filenames:
      self.CreateFile(filename)

    bucket = cloudstorage.listbucket(BUCKET + '/test/')
    self.assertEqual(filenames, [stat.filename for stat in bucket])

    bucket = cloudstorage.listbucket(BUCKET + '/test/', max_keys=1)
    stats = list(bucket)
    self.assertEqual(1, len(stats))
    stat = stats[0]
    content = ''.join(DEFAULT_CONTENT)
    self.assertEqual(filenames[0], stat.filename)
    self.assertEqual(len(content), stat.st_size)
    self.assertEqual(hashlib.md5(content).hexdigest(), stat.etag)

    bucket = cloudstorage.listbucket(BUCKET + '/test/',
                                     marker=BUCKET + '/test/foo0',
                                     max_keys=1)
    stats = [stat for stat in bucket]
    self.assertEqual(1, len(stats))
    stat = stats[0]
    self.assertEqual(foos[1], stat.filename)

  def testListBucketWithDelimiter(self):
    filenames = ['/bar',
                 '/foo0', '/foo1',
                 '/foo/a', '/foo/b/bb', '/foo/b/bbb', '/foo/c/c',
                 '/foo1/a',
                 '/foo2/a', '/foo2/b',
                 '/foo3/a']
    def FullyQualify(n):
      return BUCKET + n
    fullnames = [FullyQualify(n) for n in filenames]
    for n in fullnames:
      self.CreateFile(n)

    bucket = cloudstorage.listbucket(BUCKET + '/foo',
                                     delimiter='/',
                                     max_keys=5)
    expected = [FullyQualify(n) for n in ['/foo/', '/foo0', '/foo1',
                                          '/foo1/', '/foo2/']]
    self.assertEqual(expected, [stat.filename for stat in bucket])

    bucket = cloudstorage.listbucket(BUCKET + '/foo/',
                                     delimiter='/',
                                     max_keys=2)
    expected = [FullyQualify(n) for n in ['/foo/a', '/foo/b/']]
    self.assertEqual(expected, [stat.filename for stat in bucket])

  def testListBucketPickle(self):
    bars = [BUCKET + '/test/bar' + str(i) for i in range(3)]
    foos = [BUCKET + '/test/foo' + str(i) for i in range(3)]
    filenames = bars + foos
    for filename in filenames:
      self.CreateFile(filename)

    bucket = cloudstorage.listbucket(BUCKET + '/test/')
    self.AssertListBucketEqual(filenames, bucket)

    bucket = cloudstorage.listbucket(BUCKET + '/test/', max_keys=2)
    self.AssertListBucketEqual(bars[:2], bucket)

    bucket = cloudstorage.listbucket(BUCKET + '/test/',
                                     marker=BUCKET + '/test/bar2',
                                     max_keys=2)
    self.AssertListBucketEqual(foos[:2], bucket)

  def AssertListBucketEqual(self, expected, bucket):
    result = []
    while True:
      try:
        result.append(iter(bucket).next().filename)
        bucket = pickle.loads(pickle.dumps(bucket))
      except StopIteration:
        break
    self.assertEqual(expected, result)


class CloudStorageComposeTest(unittest.TestCase):
  """Test for Cloudstorage compose method."""

  def setUp(self):
    """Setup for Cloudstorage testing."""
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()
    self._old_max_keys = common._MAX_GET_BUCKET_RESULT
    common._MAX_GET_BUCKET_RESULT = 2
    self.start_time = time.time()
    cloudstorage.set_default_retry_params(None)
    with cloudstorage.open(TESTFILE, 'w') as gcs:
      gcs.write(DEFAULT_COMPOSE_CONTENT)

  def tearDown(self):
    """Tear down for Cloudstorage testing."""
    common._MAX_GET_BUCKET_RESULT = self._old_max_keys
    cloudstorage.delete(TESTFILE)
    self.testbed.deactivate()

  def testComposeStringForFileList(self):
    """Test to ensure TypeError is thrown if a string is sent."""
    self.assertRaises(TypeError, cloudstorage.compose, TESTFILE, DESTFILE)

  def testComposeNoneIterableForFileList(self):
    """Test to ensure TypeError is thrown if a non iterable is sent."""
    self.assertRaises(TypeError, cloudstorage.compose, 1, DESTFILE)

  def testComposeTooManyFilesFailure(self):
    """Test to ensure ValueError is thrown if more than 32 files are sent."""
    self.assertRaises(ValueError, cloudstorage.compose,
                      [TESTFILE] * 33, DESTFILE)

  def testComposeNoFilesFailure(self):
    """Test to ensure ValueError is thrown if zero paths are sent."""
    self.assertRaises(ValueError, cloudstorage.compose, [], DESTFILE)

  def testComposeOne(self):
    """Test to ensure one file can be composed (the API supports it)."""

    test_file = TESTFILE[len(BUCKET) + 1:]
    cloudstorage.compose([test_file], DESTFILE)
    with cloudstorage.open(DESTFILE, 'r') as gcs:
      results = gcs.read()
    cloudstorage.delete(DESTFILE)
    self.assertEqual(DEFAULT_COMPOSE_CONTENT, results)

  def testComposeFilesMetadataTooLargeFailure(self):
    """Test to ensure ValueError is thrown if metadata is too long."""
    self.assertRaises(ValueError, cloudstorage.compose, [TESTFILE] * 2,
                      DESTFILE, files_metadata=['a'] * 3)

  def testComposeInvalidItemInFileListFailure(self):
    """Test to ensure ValueError is thrown if there are invalid entries."""
    self.assertRaises(TypeError, cloudstorage.compose, ['1', 1], DESTFILE)

  def testCompose32Files(self):
    """Test to 32 files are composed properly."""
    test_file = TESTFILE[len(BUCKET) + 1:]
    cloudstorage.compose([test_file] * 32, DESTFILE, content_type='text/plain')

    with cloudstorage.open(DESTFILE, 'r') as gcs:
      results = gcs.read()
    cloudstorage.delete(DESTFILE)
    self.assertEqual(''.join([DEFAULT_COMPOSE_CONTENT] * 32), results)


if __name__ == '__main__':
  unittest.main()
