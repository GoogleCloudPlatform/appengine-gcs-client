# Copyright 2012 Google Inc. All Rights Reserved.

"""Python wrappers for the Google Storage RESTful API."""





__all__ = ['ReadBuffer',
           'StreamingBuffer',
          ]

import collections
import os
import urlparse
from google.appengine.ext import ndb
from cloudstorage import errors
from cloudstorage import rest_api


class _StorageApi(rest_api._RestApi):
  """A simple wrapper for the Google Storage RESTful API.

  WARNING: Do NOT directly use this api. It's an implementation detail
  and is subject to change at any release.

  All async methods have similar args and returns.

  Args:
    path: The path to the Google Storage object or bucket, e.g.
      '/mybucket/myfile' or '/mybucket'.
    **kwd: Options for urlfetch. e.g.
      headers={'content-type': 'text/plain'}, payload='blah'.

  Returns:
    A ndb Future. When fulfilled, future.get_result() should return
    a tuple of (status, headers, content) that represents a HTTP response
    of Google Cloud Storage XML API.
  """

  api_url = 'https://storage.googleapis.com'
  read_only_scope = 'https://www.googleapis.com/auth/devstorage.read_only'
  read_write_scope = 'https://www.googleapis.com/auth/devstorage.read_write'
  full_control_scope = 'https://www.googleapis.com/auth/devstorage.full_control'


  def post_object_async(self, path, **kwds):
    """POST to an object."""
    return self.do_request_async(self.api_url + path, 'POST', **kwds)

  def put_object_async(self, path, **kwds):
    """PUT an object."""
    return self.do_request_async(self.api_url + path, 'PUT', **kwds)

  def get_object_async(self, path, **kwds):
    """GET an object.

    Note: No payload argument is supported.
    """
    return self.do_request_async(self.api_url + path, 'GET', **kwds)

  def delete_object_async(self, path, **kwds):
    """DELETE an object.

    Note: No payload argument is supported.
    """
    return self.do_request_async(self.api_url + path, 'DELETE', **kwds)

  def head_object_async(self, path, **kwds):
    """HEAD an object.

    Depending on request headers, HEAD returns various object properties,
    e.g. Content-Length, Last-Modified, and ETag.

    Note: No payload argument is supported.
    """
    return self.do_request_async(self.api_url + path, 'HEAD', **kwds)

  def get_bucket_async(self, path, **kwds):
    """GET a bucket."""
    return self.do_request_async(self.api_url + path, 'GET', **kwds)



_StorageApi = rest_api.add_sync_methods(_StorageApi)


class ReadBuffer(object):
  """A class for reading Google storage files."""

  DEFAULT_BUFFER_SIZE = 1024 * 1024
  MAX_REQUEST_SIZE = 30 * DEFAULT_BUFFER_SIZE

  def __init__(self,
               api,
               path,
               max_buffer_size=DEFAULT_BUFFER_SIZE,
               max_request_size=MAX_REQUEST_SIZE):
    """Constructor.

    Args:
      api: A StorageApi instance.
      path: Path to the object, e.g. '/mybucket/myfile'.
      max_buffer_size: Max bytes to buffer.
      max_request_size: Max bytes to request in one urlfetch.
    """
    self._api = api
    self._path = path
    self._max_buffer_size = max_buffer_size
    self._max_request_size = max_request_size
    self._offset = 0
    self._buffer = ''
    self._buffer_offset = 0
    self._closed = False
    self._buffer_future = self._get_segment(0, self._max_buffer_size)
    self._file_size = None
    self._etag = None

  def readline(self, size=-1):
    """Read one line delimited by '\n' from the file.

    A trailing newline character is kept in the string. It may be absent when a
    file ends with an incomplete line. If the size argument is non-negative,
    it specifies the maximum string size (counting the newline) to return.
    A negative size is the same as unspecified. Empty string is returned
    only when EOF is encountered immediately.

    Args:
      size: Maximum number of bytes to read. If not specified, readline stops
        only on '\n' or EOF.

    Returns:
      The data read as a string.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    if self._file_size is None:
      self._buffer_future.get_result()
    self._buffer_future = None

    data_list = []

    if size == 0:
      return ''

    while True:
      if size >= 0:
        end_offset = self._buffer_offset + size
      else:
        end_offset = len(self._buffer)
      newline_offset = self._buffer.find('\n', self._buffer_offset, end_offset)

      if newline_offset >= 0:
        data_list.append(
            self._read_buffer(newline_offset + 1 - self._buffer_offset))
        return ''.join(data_list)
      else:
        result = self._read_buffer(size)
        data_list.append(result)
        size -= len(result)
        if size == 0 or self._file_size == self._offset:
          return ''.join(data_list)
        self._fill_buffer()

  def read(self, size=-1):
    """Read data from RAW file.

    Args:
      size: Number of bytes to read as integer. Actual number of bytes
        read is always equal to size unless EOF is reached. If size is
        negative or unspecified, read the entire file.

    Returns:
      data read as str.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    if size >= 0 and size <= len(self._buffer) - self._buffer_offset:
      result = self._read_buffer(size)
    else:
      size -= len(self._buffer) - self._buffer_offset
      data_list = [self._read_buffer()]

      if self._buffer_future:
        self._reset_buffer(self._buffer_future.get_result())
        self._buffer_future = None

      if size >= 0 and size <= len(self._buffer) - self._buffer_offset:
        data_list.append(self._read_buffer(size))
      else:
        size -= len(self._buffer)
        data_list.append(self._read_buffer())
        if self._offset == self._file_size:
          return ''.join(data_list)

        if size < 0 or size >= self._file_size - self._offset:
          needs = self._file_size - self._offset
        else:
          needs = size
        data_list.extend(self._get_segments(self._offset, needs))
        self._offset += needs
      result = ''.join(data_list)

    assert self._buffer_future is None
    if self._offset != self._file_size and not self._buffer:
      self._buffer_future = self._get_segment(self._offset,
                                              self._max_buffer_size)
    return result

  def _read_buffer(self, size=-1):
    """Returns bytes from self._buffer and update related offsets.

    Args:
      size: number of bytes to read. Read the entire buffer if negative.

    Returns:
      Requested bytes from buffer.
    """
    if size < 0:
      size = len(self._buffer) - self._buffer_offset
    result = self._buffer[self._buffer_offset : self._buffer_offset+size]
    self._offset += len(result)
    self._buffer_offset += len(result)
    if self._buffer_offset == len(self._buffer):
      self._reset_buffer()
    return result

  def _fill_buffer(self):
    """Fill self._buffer."""
    segments = self._get_segments(self._offset,
                                  min(self._max_buffer_size,
                                      self._max_request_size,
                                      self._file_size-self._offset))

    self._reset_buffer(''.join(segments))

  def _get_segments(self, start, request_size):
    """Get segments of the file from Google Storage as a list.

    A large request is broken into segments to avoid hitting urlfetch
    response size limit. Each segment is returned from a separate urlfetch.

    Args:
      start: start offset to request. Inclusive. Have to be within the
        range of the file.
      request_size: number of bytes to request. Can not exceed the logical
        range of the file.

    Returns:
      A list of file segments in order
    """
    end = start + request_size
    futures = []

    while request_size > self._max_request_size:
      futures.append(self._get_segment(start, self._max_request_size))
      request_size -= self._max_request_size
      start += self._max_request_size
    if start < end:
      futures.append(self._get_segment(start, end-start))
    return [fut.get_result() for fut in futures]

  @ndb.tasklet
  def _get_segment(self, start, request_size):
    """Get a segment of the file from Google Storage.

    Args:
      start: start offset of the segment. Inclusive. Have to be within the
        range of the file.
      request_size: number of bytes to request. Have to be within the range
        of the file.

    Yields:
      a segment [start, start + request_size) of the file.

    Raises:
      ValueError: if the file has changed while reading.
    """
    end = start + request_size - 1
    content_range = '%d-%d' % (start, end)
    headers = {'Range': 'bytes=' + content_range}
    status, headers, content = yield self._api.get_object_async(self._path,
                                                                headers=headers)
    errors.check_status(status, [200, 206], headers)
    if self._file_size is None:
      self._file_size = self._get_file_size(headers.get('content-range'))
      self._etag = headers.get('etag')
    else:
      if self._etag != headers.get('etag'):
        raise ValueError('File on GCS has changed while reading.')
    raise ndb.Return(content)

  def close(self):
    self._closed = True
    self._reset_buffer()

  def seek(self, offset, whence=os.SEEK_SET):
    """Set the file's current offset.

    Note if the new offset is out of bound, it is adjusted to either 0 or EOF.

    Args:
      offset: seek offset as number.
      whence: seek mode. Supported modes are os.SEEK_SET (absolute seek),
        os.SEEK_CUR (seek relative to the current position), and os.SEEK_END
        (seek relative to the end, offset should be negative).

    Raises:
      IOError: When this buffer is closed.
      ValueError: When whence is invalid.
    """
    self._check_open()

    self._reset_buffer()
    if self._file_size is None:
      self._buffer_future.get_result()
    self._buffer_future = None

    if whence == os.SEEK_SET:
      self._offset = offset
    elif whence == os.SEEK_CUR:
      self._offset += offset
    elif whence == os.SEEK_END:
      self._offset = self._file_size + offset
    else:
      raise ValueError('Whence mode %s is invalid.' % str(whence))

    self._offset = min(self._offset, self._file_size)
    self._offset = max(self._offset, 0)
    if self._offset != self._file_size:
      self._buffer_future = self._get_segment(self._offset,
                                              self._max_buffer_size)

  def tell(self):
    """Tell the file's current offset.

    Returns:
      current offset in reading this file.

    Raises:
      IOError: When this buffer is closed.
    """
    self._check_open()
    return self._offset

  def _check_open(self):
    if self._closed:
      raise IOError('Buffer is closed.')

  def _reset_buffer(self, new_buffer='', buffer_offset=0):
    self._buffer = new_buffer
    self._buffer_offset = buffer_offset

  def _get_file_size(self, content_range):
    """Get total file size from content-range header.

    Args:
      content_range: of format 'content-range': 'bytes 0-10/55'
    """
    return long(content_range.rsplit('/', 1)[-1])

  def _is_eof(self, content_range):
    """Test if a urlfetch request has reached the end of this file.

    Args:
      content_range: of format 'content-range': 'bytes 0-54/55'
    """
    end_offset = long(content_range[content_range.rfind('-') + 1,
                                    content_range.rfind('/')])
    file_size = self._get_file_size(content_range)
    return end_offset == file_size - 1


class StreamingBuffer(object):
  """A class for creating large objects using the 'resumable' API.

  The API is a subset of the Python writable stream API sufficient to
  support writing zip files using the zipfile module.

  The exact sequence of calls and use of headers is documented at
  https://developers.google.com/storage/docs/developer-guide#unknownresumables
  """

  _blocksize = 256 * 1024

  _maxrequestsize = 16 * _blocksize

  def __init__(self,
               api,
               path,
               content_type=None,
               gs_headers=None):
    """Constructor.

    Args:
      api: A StorageApi instance.
      path: Path to the object, e.g. '/mybucket/myfile'.
      content_type: Optional content-type; Default value is
        delegate to Google Cloud Storage.
      gs_headers: additional gs headers as a str->str dict, e.g
        {'x-goog-acl': 'private', 'x-goog-meta-foo': 'foo'}.
    """
    assert self._maxrequestsize > self._blocksize
    assert self._maxrequestsize % self._blocksize == 0

    self._api = api
    self._path = path

    self._buffer = collections.deque()
    self._buffered = 0
    self._written = 0
    self._offset = 0

    self._closed = False

    headers = {'x-goog-resumable': 'start'}
    if content_type:
      headers['content-type'] = content_type
    if gs_headers:
      headers.update(gs_headers)
    status, headers, _ = self._api.post_object(path, headers=headers)
    errors.check_status(status, [201], headers)
    loc = headers.get('location')
    if not loc:
      raise IOError('No location header found in 201 response')
    parsed = urlparse.urlparse(loc)
    self._path_with_token = '%s?%s' % (self._path, parsed.query)

  def write(self, data):
    """Write some bytes."""
    self._check_open()
    assert isinstance(data, str)
    if not data:
      return
    self._buffer.append(data)
    self._buffered += len(data)
    self._offset += len(data)
    if self._buffered >= self._blocksize:
      self._flush()

  def flush(self):
    """Dummy API.

    This API is provided because the zipfile module uses it.  It is a
    no-op because Google Storage *requires* that all writes except for
    the final one are multiples on 256K bytes aligned on 256K-byte
    boundaries.
    """
    self._check_open()

  def tell(self):
    """Return the total number of bytes passed to write() so far.

    (There is no seek() method.)
    """
    self._check_open()
    return self._offset

  def close(self):
    """Flush the buffer and finalize the file.

    When this returns the new file is available for reading.
    """
    self._closed = True
    self._flush(finish=True)

  def _flush(self, finish=False):
    """Internal API to flush.

    This is called only when the total amount of buffered data is at
    least self._blocksize, or to flush the final (incomplete) block of
    the file with finish=True.
    """
    if finish:
      level = 1
    else:
      level = self._blocksize

    last = False
    while self._buffered >= level:
      buffer = []
      buffered = 0

      while self._buffer:
        buf = self._buffer.popleft()
        size = len(buf)
        self._buffered -= size
        buffer.append(buf)
        buffered += size
        if buffered >= self._maxrequestsize:
          break

      if buffered > self._maxrequestsize:
        excess = buffered - self._maxrequestsize
      elif finish:
        excess = 0
      else:
        excess = buffered % self._blocksize

      if excess:
        over = buffer.pop()
        size = len(over)
        assert size >= excess
        buffered -= size
        head, tail = over[:-excess], over[-excess:]
        self._buffer.appendleft(tail)
        self._buffered += len(tail)
        if head:
          buffer.append(head)
          buffered += len(head)

      if finish:
        last = not self._buffered
      self._send_data(''.join(buffer), last)

    if finish and not last:
      self._send_data('', True)

  def _send_data(self, data, last):
    """Send the block to the storage service and update self._written."""
    headers = {}
    if data:
      length = self._written + len(data)
      headers['content-range'] = ('bytes %d-%d/%s' %
                                  (self._written, length-1,
                                   length if last else '*'))
    status, _, _ = self._api.put_object(
        self._path_with_token, payload=data, headers=headers)
    if last:
      expected = 200
    else:
      expected = 308
    errors.check_status(status, [expected], headers)
    self._written += len(data)

  def _check_open(self):
    if self._closed:
      raise IOError('Buffer is closed.')
