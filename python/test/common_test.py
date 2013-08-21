# Copyright 2012 Google Inc. All Rights Reserved.

"""Tests for common.py."""



import math
import sys
import time
import unittest
import mock



try:
  from cloudstorage import common
except ImportError:
  from google.appengine.ext.cloudstorage import common
from google.appengine.tools import remote_api_shell


class HelpersTest(unittest.TestCase):
  """Test for common helpers."""

  def testValidateBucketName(self):
    self.assertRaises(ValueError, common.validate_bucket_name, 'c' * 2)
    self.assertRaises(ValueError, common.validate_bucket_name, 'c' * 64)
    self.assertRaises(ValueError, common.validate_bucket_name, 'CCC')
    self.assertRaises(ValueError, common.validate_bucket_name, 'ccc%')
    self.assertRaises(ValueError, common.validate_bucket_name, 'ccc/')
    self.assertRaises(ValueError, common.validate_bucket_name, 'ccc\\')
    common.validate_bucket_name('c' * 3)
    common.validate_bucket_name('c' * 63)
    common.validate_bucket_name('abc-_.123')

  def testValidatePath(self):
    self.assertRaises(ValueError, common.validate_bucket_path, '/bucke*')
    self.assertRaises(ValueError, common.validate_file_path, None)
    self.assertRaises(ValueError, common.validate_file_path, '/bucketabcd')
    self.assertRaises(TypeError, common.validate_file_path, 1)
    common.validate_file_path('/bucket/file')
    common.validate_file_path('/bucket/dir/dir2/file')
    common.validate_file_path('/bucket/dir/dir2/file' + 'c' * 64)

  def testValidateGcsOptions(self):
    self.assertRaises(TypeError, common.validate_options, {1: 'foo'})
    self.assertRaises(ValueError, common.validate_options, {'foo': 1})
    self.assertRaises(ValueError, common.validate_options, {'foo': 'bar'})
    common.validate_options({'x-goog-meta-foo': 'foo',
                             'x-goog-meta-bar': 'bar',
                             'x-goog-acl': 'private'})

  def testTimeConversion(self):
    posix_time = 1354144241.0
    http_time = 'Wed, 28 Nov 2012 23:10:41 GMT'
    self.assertEqual(http_time, common.posix_time_to_http(posix_time))
    self.assertEqual(posix_time, common.http_time_to_posix(http_time))

    posix = math.floor(time.time())
    http_time = common.posix_time_to_http(posix)
    self.assertEqual('GMT', http_time[-3:])
    new_posix = common.http_time_to_posix(http_time)
    self.assertEqual(posix, new_posix)

  def testDatetimeConversion(self):
    dt_str = '2013-04-12T00:22:27.000Z'
    self.assertEqual(dt_str,
                     common.posix_to_dt_str(common.dt_str_to_posix(dt_str)))

  def testLocalRunOnRemoteAPIShell(self):
    with mock.patch('google.appengine.tools.remote_api_shell'
                    '.remote_api_stub') as _:
      with mock.patch('google.appengine.tools.remote_api_shell'
                      '.code') as _:
        sys.argv = ['shell.py', '-s', 'app_id.appspot.com', 'app_id']
        remote_api_shell.main(sys.argv)
        self.assertFalse(common.local_run())

if __name__ == '__main__':
  unittest.main()
