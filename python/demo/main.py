# Copyright 2012 Google Inc. All Rights Reserved.


"""A sample app that uses GCS client to operate on bucket and file."""

import os
import cloudstorage as gcs
import webapp2

"""
Retry can help overcome transient urlfetch or GCS issues, such as timeouts.
"""
my_default_retry_params = gcs.RetryParams(initial_delay=0.2,
                                          max_delay=5.0,
                                          backoff_factor=2,
                                          max_retry_period=15)
"""
All requests to GCS using the GCS client within current GAE request and
current thread will use this retry params as default. A specific GCS
client call can override the default.
"""
gcs.set_default_retry_params(my_default_retry_params)


class MainPage(webapp2.RequestHandler):

  def get(self):
    bucket = '/yey-cloud-storage-trial'
    filename = bucket + '/demo-testfile'

    self.response.headers['Content-Type'] = 'text/plain'

    self.create_file(filename)
    self.response.write('\n\n')

    self.read_file(filename)
    self.response.write('\n\n')

    self.stat_file(filename)
    self.response.write('\n\n')

    self.list_bucket(bucket)
    self.response.write('\n\n')

    self.delete_file(filename)

  def create_file(self, filename):
    """Create a file.

    The retry_params specified in the open call will override the default
    retry params for this particular file handle.

    Args:
      filename: filename.
    """
    self.response.write('Creating file...\n')

    write_retry_params = gcs.RetryParams(backoff_factor=1.1)
    gcs_file = gcs.open(filename,
                        'w',
                        content_type='text/plain',
                        options={'x-goog-meta-foo': 'foo',
                                 'x-goog-meta-bar': 'bar'},
                        retry_params=write_retry_params)
    gcs_file.write('abcde\n')
    gcs_file.write('f'*1024*1024 + '\n')
    gcs_file.close()

  def read_file(self, filename):
    self.response.write('Truncated file content:\n')

    gcs_file = gcs.open(filename)
    self.response.write(gcs_file.readline())
    gcs_file.seek(-1024, os.SEEK_END)
    self.response.write(gcs_file.read())
    gcs_file.close()

  def stat_file(self, filename):
    self.response.write('File stat:\n')

    stat = gcs.stat(filename)
    self.response.write(repr(stat))

  def list_bucket(self, bucket):
    self.response.write('Listbucket result:\n')

    stats = gcs.listbucket(bucket)
    for stat in stats:
      self.response.write(repr(stat))
      self.response.write('\n')

  def delete_file(self, filename):
    self.response.write('Deleting file...\n')
    gcs.delete(filename)

    try:
      gcs.delete(filename)
    except gcs.NotFoundError:
      pass


app = webapp2.WSGIApplication([('/', MainPage)],
                              debug=True)
