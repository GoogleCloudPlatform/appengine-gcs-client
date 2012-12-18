# Copyright 2012 Google Inc. All Rights Reserved.

"""Helpers shared by cloudstorage_stub and cloudstorage_api."""





__all__ = ['CS_XML_NS',
           'CSFileStat',
           'LOCAL_API_URL',
           'get_access_token',
           'get_metadata',
           'http_time_to_posix',
           'posix_time_to_http',
           'set_access_token',
           'validate_options',
           'validate_bucket_path',
           'validate_file_path',
          ]

from email import utils as email_utils
import re

_CS_BUCKET_REGEX = re.compile(r'/[a-z0-9\.\-_]{3,}$')
_CS_FULLPATH_REGEX = re.compile(r'/[a-z0-9\.\-_]{3,}/.*')
_CS_OPTIONS = ('x-goog-acl',
               'x-goog-meta-')
CS_XML_NS = 'http://doc.s3.amazonaws.com/2006-03-01'
LOCAL_API_URL = 'http://gcs-magicstring.appspot.com'
_access_token = ''


def set_access_token(access_token):
  """Set the shared access token to authenticate with Cloud Storage.

  When set, the library will always attempt to communicate with the
  real Cloud Storage with this token even when running on dev appserver.
  Note the token could expire so it's up to you to renew it.

  When absent, the library will automatically request and refresh a token
  on appserver, or when on dev appserver, talk to a Cloud Storage
  stub.

  Args:
    access_token: you can get one by run 'gsutil -d ls' and copy the
      str after 'Bearer'.
  """
  global _access_token
  _access_token = access_token


def get_access_token():
  """Returns the shared access token."""
  return _access_token


class CSFileStat(object):
  """Container for CS file stat."""

  def __init__(self,
               filename,
               st_size,
               etag,
               st_ctime=None,
               content_type=None,
               metadata=None):
    """Initialize.

    Args:
      filename: a Google Storage filename of form '/bucket/filename'.
      st_size: file size in bytes. long.
      etag: hex digest of the md5 hash of the file's content. str.
      st_ctime: posix file creation time. float.
      content_type: content type. str.
      metadata: a str->str dict of user specified metadata from the
        x-goog-meta header, e.g. {'x-goog-meta-foo': 'foo'}.
    """
    self.filename = filename
    self.st_size = st_size
    self.st_ctime = st_ctime
    if etag[0] == '"' and etag[-1] == '"':
      etag = etag[1:-1]
    self.etag = etag
    self.content_type = content_type
    self.metadata = metadata

  def __repr__(self):
    return (
        '(filename: %(filename)s, st_size: %(st_size)s, '
        'st_ctime: %(st_ctime)s, etag: %(etag)s, '
        'content_type: %(content_type)s, '
        'metadata: %(metadata)s)' %
        dict(filename=self.filename,
             st_size=self.st_size,
             st_ctime=self.st_ctime,
             etag=self.etag,
             content_type=self.content_type,
             metadata=self.metadata))


def get_metadata(headers):
  """Get user defined metadata from HTTP response headers."""
  return dict((k, v) for k, v in headers.iteritems()
              if k.startswith('x-goog-meta-'))


def validate_bucket_path(path):
  """Validate a Google Storage bucket path.

  Args:
    path: a Google Storage bucket path. It should have form '/bucket'.
    is_bucket: whether this is a bucket path or file path.

  Raises:
    ValueError: if path is invalid.
  """
  _validate_path(path)
  if not _CS_BUCKET_REGEX.match(path):
    raise ValueError('Bucket should have format /bucket '
                     'but got %s' % path)


def validate_file_path(path):
  """Validate a Google Storage file path.

  Args:
    path: a Google Storage file path. It should have form '/bucket/filename'.

  Raises:
    ValueError: if path is invalid.
  """
  _validate_path(path)
  if not _CS_FULLPATH_REGEX.match(path):
    raise ValueError('Path should have format /bucket/filename '
                     'but got %s' % path)


def _validate_path(path):
  """Basic validation of Google Storage paths.

  Args:
    path: a Google Storage path. It should have form '/bucket/filename'
      or '/bucket'.

  Raises:
    ValueError: if path is invalid.
    TypeError: if path is not of type basestring.
  """
  if not path:
    raise ValueError('Path is empty')
  if not isinstance(path, basestring):
    raise TypeError('Path should be a string but is %s (%s).' %
                    (path.__class__, path))


def validate_options(options):
  """Validate Cloud Storage options.

  Args:
    options: a str->basestring dict of options to pass to Cloud Storage.

  Raises:
    ValueError: if option is not supported.
    TypeError: if option is not of type str or value of an option
      is not of type basestring.
  """
  if not options:
    return

  for k, v in options.iteritems():
    if not isinstance(k, str):
      raise TypeError('option %r should be a str.' % k)
    if not any(k.startswith(valid) for valid in _CS_OPTIONS):
      raise ValueError('option %s is not supported.' % k)
    if not isinstance(v, basestring):
      raise TypeError('value %r for option %s should be of type basestring.' %
                      v, k)


def http_time_to_posix(http_time):
  """Convert HTTP time format to posix time.

  Args:
    http_time: time in RFC 2822 format. e.g.
      "Mon, 20 Nov 1995 19:12:08 GMT".

  Returns:
    A float of secs from unix epoch.
  """
  if http_time:
    return email_utils.mktime_tz(email_utils.parsedate_tz(http_time))


def posix_time_to_http(posix_time):
  """Convert posix time to HTML header time format."""
  if posix_time:
    return email_utils.formatdate(posix_time, usegmt=True)
