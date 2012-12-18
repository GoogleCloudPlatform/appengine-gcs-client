# Copyright 2012 Google Inc. All Rights Reserved.

"""Testing for Google Storage stub."""



import unittest
from google.appengine.ext import testbed
from cloudstorage import cloudstorage_stub


class CloudStorageTest(unittest.TestCase):
  """Test CloudStorage stub.

  Create a gs stub and run gs stub through its flow.
  """

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_app_identity_stub()
    self.testbed.init_blobstore_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()
    self.storage = cloudstorage_stub.CloudStorageStub(
        self.testbed.get_stub('blobstore').storage)

  def tearDown(self):
    self.testbed.deactivate()

  def testObjectFlow(self):
    """Get object operations."""
    filename = '/bucket/filename'
    gs_options = {'x-goog-meta-foo': 'foo',
                  'x-GOOG-meta-bar': 'bar',
                  'content-type': 'text/plain'}

    token = self.storage.post_start_creation(filename, gs_options)
    self.storage.put_continue_creation(token, 'abcde', (0, 4))
    self.storage.put_continue_creation(token, 'f', (5, 5))
    self.storage.put_continue_creation(token, 'g', (6, 6), True)

    self.assertEqual('abcdefg', self.storage.get_object(filename))
    self.assertEqual('cdef', self.storage.get_object(filename, 2, 5))

    fileinfo = self.storage.head_object(filename)
    self.assertEqual(7, fileinfo.st_size)
    self.assertEqual(filename, fileinfo.filename)
    self.assertEqual('bar', fileinfo.metadata['x-goog-meta-bar'])
    self.assertEqual('foo', fileinfo.metadata['x-goog-meta-foo'])
    self.assertEqual('text/plain', fileinfo.content_type)

    self.assertTrue(self.storage.delete_object(filename))
    self.assertEqual(None, self.storage.head_object(filename))
    self.assertFalse(self.storage.delete_object(filename))
    self.assertEqual(None, self.storage.head_object(filename))

  def testRecreateExistingFile(self):
    filename = '/bucket/filename'

    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'abcde', (0, 4), True)
    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'aaaaa', (0, 4), True)

    self.assertEqual('aaaaa', self.storage.get_object(filename, 0))

    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'aaaaa', (0, 4))
    self.storage.put_continue_creation(token, 'bbbbb', (5, 9))
    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'abcde', (0, 4), True)

    self.assertEqual('abcde', self.storage.get_object(filename, 0))

  def testCorruptedUploads(self):
    filename = '/bucket/filename'

    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'abcde', (0, 4))
    self.assertRaises(ValueError,
                      self.storage.put_continue_creation,
                      token, 'efg', (4, 6), True)

    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'abcde', (0, 4))
    self.assertRaises(ValueError,
                      self.storage.put_continue_creation,
                      token, 'efg', (6, 8), True)

    token = self.storage.post_start_creation(filename, {})
    self.storage.put_continue_creation(token, 'abcde', (0, 4))
    self.assertRaises(ValueError,
                      self.storage.put_continue_creation,
                      token, 'efg', (6, 7), True)

  def CreateFile(self, path):
    token = self.storage.post_start_creation(path, {})
    self.storage.put_continue_creation(token, 'abcde', (0, 4), True)

  def testGetBucket(self):
    """Test get bucket."""
    bars = ['/bucket/bar%d' % i for i in range(5)]
    foos = ['/bucket/foo%d' % i for i in range(5)]
    filenames = bars + foos
    for filename in filenames:
      self.CreateFile(filename)

    results = self.storage.get_bucket('/bucket', '', '', 100)
    self.assertEqual(10, len(results))
    self.assertEqual(filenames, [stat.filename for stat in results])

    results = self.storage.get_bucket('/bucket',
                                      prefix='foo',
                                      marker='',
                                      max_keys=100)
    self.assertEqual(5, len(results))
    self.assertEqual(foos, [stat.filename for stat in results])

    results = self.storage.get_bucket('/bucket',
                                      prefix='foo',
                                      marker='',
                                      max_keys=1)
    self.assertEqual(1, len(results))
    stat = results[0]
    self.assertEqual('/bucket/foo0', stat.filename)
    self.assertEqual(5, stat.st_size)

    results = self.storage.get_bucket('/bucket',
                                      prefix='foo',
                                      marker='foo3',
                                      max_keys=100)
    stat = results[0]
    self.assertEqual('/bucket/foo4', stat.filename)

    results = self.storage.get_bucket('/bucket',
                                      prefix='',
                                      marker='foo3',
                                      max_keys=100)
    stat = results[0]
    self.assertEqual('/bucket/foo4', stat.filename)


if __name__ == '__main__':
  unittest.main()
