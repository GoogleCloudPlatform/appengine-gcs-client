# Copyright 2012 Google Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific
# language governing permissions and limitations under the License.

"""File Interface for Google Cloud Storage."""



from __future__ import with_statement



__all__ = ['delete',
           'listbucket',
           'open',
           'stat',
           'compose',
          ]

import logging
import StringIO
import urllib
import os
import xml.etree.cElementTree as ET
from . import api_utils
from . import common
from . import errors
from . import storage_api



def open(filename,
         mode='r',
         content_type=None,
         options=None,
         read_buffer_size=storage_api.ReadBuffer.DEFAULT_BUFFER_SIZE,
         retry_params=None,
         _account_id=None):
  """Opens a Google Cloud Storage file and returns it as a File-like object.

  Args:
    filename: A Google Cloud Storage filename of form '/bucket/filename'.
    mode: 'r' for reading mode. 'w' for writing mode.
      In reading mode, the file must exist. In writing mode, a file will
      be created or be overrode.
    content_type: The MIME type of the file. str. Only valid in writing mode.
    options: A str->basestring dict to specify additional headers to pass to
      GCS e.g. {'x-goog-acl': 'private', 'x-goog-meta-foo': 'foo'}.
      Supported options are x-goog-acl, x-goog-meta-, cache-control,
      content-disposition, and content-encoding.
      Only valid in writing mode.
      See https://developers.google.com/storage/docs/reference-headers
      for details.
    read_buffer_size: The buffer size for read. Read keeps a buffer
      and prefetches another one. To minimize blocking for large files,
      always read by buffer size. To minimize number of RPC requests for
      small files, set a large buffer size. Max is 30MB.
    retry_params: An instance of api_utils.RetryParams for subsequent calls
      to GCS from this file handle. If None, the default one is used.
    _account_id: Internal-use only.

  Returns:
    A reading or writing buffer that supports File-like interface. Buffer
    must be closed after operations are done.

  Raises:
    errors.AuthorizationError: if authorization failed.
    errors.NotFoundError: if an object that's expected to exist doesn't.
    ValueError: invalid open mode or if content_type or options are specified
      in reading mode.
  """
  common.validate_file_path(filename)
  api = storage_api._get_storage_api(retry_params=retry_params,
                                     account_id=_account_id)
  filename = api_utils._quote_filename(filename)

  if mode == 'w':
    common.validate_options(options)
    return storage_api.StreamingBuffer(api, filename, content_type, options)
  elif mode == 'r':
    if content_type or options:
      raise ValueError('Options and content_type can only be specified '
                       'for writing mode.')
    return storage_api.ReadBuffer(api,
                                  filename,
                                  buffer_size=read_buffer_size)
  else:
    raise ValueError('Invalid mode %s.' % mode)


def delete(filename, retry_params=None, _account_id=None):
  """Delete a Google Cloud Storage file.

  Args:
    filename: A Google Cloud Storage filename of form '/bucket/filename'.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
      the default one is used.
    _account_id: Internal-use only.

  Raises:
    errors.NotFoundError: if the file doesn't exist prior to deletion.
  """
  api = storage_api._get_storage_api(retry_params=retry_params,
                                     account_id=_account_id)
  common.validate_file_path(filename)
  filename = api_utils._quote_filename(filename)
  status, resp_headers, content = api.delete_object(filename)
  errors.check_status(status, [204], filename, resp_headers=resp_headers,
                      body=content)


def stat(filename, retry_params=None, _account_id=None):
  """Get GCSFileStat of a Google Cloud storage file.

  Args:
    filename: A Google Cloud Storage filename of form '/bucket/filename'.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
      the default one is used.
    _account_id: Internal-use only.

  Returns:
    a GCSFileStat object containing info about this file.

  Raises:
    errors.AuthorizationError: if authorization failed.
    errors.NotFoundError: if an object that's expected to exist doesn't.
  """
  common.validate_file_path(filename)
  api = storage_api._get_storage_api(retry_params=retry_params,
                                     account_id=_account_id)
  status, headers, content = api.head_object(
      api_utils._quote_filename(filename))
  errors.check_status(status, [200], filename, resp_headers=headers,
                      body=content)
  file_stat = common.GCSFileStat(
      filename=filename,
      st_size=common.get_stored_content_length(headers),
      st_ctime=common.http_time_to_posix(headers.get('last-modified')),
      etag=headers.get('etag'),
      content_type=headers.get('content-type'),
      metadata=common.get_metadata(headers))

  return file_stat


def _copy2(src, dst, metadata=None, retry_params=None):
  """Copy the file content from src to dst.

  Internal use only!

  Args:
    src: /bucket/filename
    dst: /bucket/filename
    metadata: a dict of metadata for this copy. If None, old metadata is copied.
      For example, {'x-goog-meta-foo': 'bar'}.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
      the default one is used.

  Raises:
    errors.AuthorizationError: if authorization failed.
    errors.NotFoundError: if an object that's expected to exist doesn't.
  """
  common.validate_file_path(src)
  common.validate_file_path(dst)

  if metadata is None:
    metadata = {}
    copy_meta = 'COPY'
  else:
    copy_meta = 'REPLACE'
  metadata.update({'x-goog-copy-source': src,
                   'x-goog-metadata-directive': copy_meta})

  api = storage_api._get_storage_api(retry_params=retry_params)
  status, resp_headers, content = api.put_object(
      api_utils._quote_filename(dst), headers=metadata)
  errors.check_status(status, [200], src, metadata, resp_headers, body=content)


def listbucket(path_prefix, marker=None, prefix=None, max_keys=None,
               delimiter=None, retry_params=None, _account_id=None):
  """Returns a GCSFileStat iterator over a bucket.

  Optional arguments can limit the result to a subset of files under bucket.

  This function has two modes:
  1. List bucket mode: Lists all files in the bucket without any concept of
     hierarchy. GCS doesn't have real directory hierarchies.
  2. Directory emulation mode: If you specify the 'delimiter' argument,
     it is used as a path separator to emulate a hierarchy of directories.
     In this mode, the "path_prefix" argument should end in the delimiter
     specified (thus designates a logical directory). The logical directory's
     contents, both files and subdirectories, are listed. The names of
     subdirectories returned will end with the delimiter. So listbucket
     can be called with the subdirectory name to list the subdirectory's
     contents.

  Args:
    path_prefix: A Google Cloud Storage path of format "/bucket" or
      "/bucket/prefix". Only objects whose fullpath starts with the
      path_prefix will be returned.
    marker: Another path prefix. Only objects whose fullpath starts
      lexicographically after marker will be returned (exclusive).
    prefix: Deprecated. Use path_prefix.
    max_keys: The limit on the number of objects to return. int.
      For best performance, specify max_keys only if you know how many objects
      you want. Otherwise, this method requests large batches and handles
      pagination for you.
    delimiter: Use to turn on directory mode. str of one or multiple chars
      that your bucket uses as its directory separator.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
      the default one is used.
    _account_id: Internal-use only.

  Examples:
    For files "/bucket/a",
              "/bucket/bar/1"
              "/bucket/foo",
              "/bucket/foo/1", "/bucket/foo/2/1", "/bucket/foo/3/1",

    Regular mode:
    listbucket("/bucket/f", marker="/bucket/foo/1")
    will match "/bucket/foo/2/1", "/bucket/foo/3/1".

    Directory mode:
    listbucket("/bucket/", delimiter="/")
    will match "/bucket/a, "/bucket/bar/" "/bucket/foo", "/bucket/foo/".
    listbucket("/bucket/foo/", delimiter="/")
    will match "/bucket/foo/1", "/bucket/foo/2/", "/bucket/foo/3/"

  Returns:
    Regular mode:
    A GCSFileStat iterator over matched files ordered by filename.
    The iterator returns GCSFileStat objects. filename, etag, st_size,
    st_ctime, and is_dir are set.

    Directory emulation mode:
    A GCSFileStat iterator over matched files and directories ordered by
    name. The iterator returns GCSFileStat objects. For directories,
    only the filename and is_dir fields are set.

    The last name yielded can be used as next call's marker.
  """
  if prefix:
    common.validate_bucket_path(path_prefix)
    bucket = path_prefix
  else:
    bucket, prefix = common._process_path_prefix(path_prefix)

  if marker and marker.startswith(bucket):
    marker = marker[len(bucket) + 1:]

  api = storage_api._get_storage_api(retry_params=retry_params,
                                     account_id=_account_id)
  options = {}
  if marker:
    options['marker'] = marker
  if max_keys:
    options['max-keys'] = max_keys
  if prefix:
    options['prefix'] = prefix
  if delimiter:
    options['delimiter'] = delimiter

  return _Bucket(api, bucket, options)


# pylint: disable=too-many-locals, too-many-branches, too-many-statements
def compose(list_of_files, destination_file,
            content_type=None, retry_params=None, _account_id=None):
  '''
    Runs the GCS Compose on the inputed files.
    Merges between 2 and 32 files into one file. If any of the previous files had the merge
    run on them it counts towards the 32 limit.
  Args:
    list_of_files: list of file name strings or dictionaries with the following format:
      {'file_name': REQUIRED name of the file to be merged. Do not include the bucket name,
       'Generation': OPTIONAL Used to specify what version of a file to use,
       'IfGenerationMatch': OPTIONAL Used to fail requests if versions don't match}
    destination_file: Path to the desired output file. Must have the bucket in the path.
    content_type: Used to specify the content-header of the output.
                  If None will try to guess based off the first file.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
                  the default one is used.
    _account_id: Internal-use only.

  Raises:
    TypeError: If the dictionary for the file list is malformed
    ValueError: If the number of files is outside the range of 2-1024
    errors.NotFoundError: If any element in the file list is missing the 'file_name' key
  '''
  api = storage_api._get_storage_api(retry_params=retry_params,
                                   account_id=_account_id)
  '''
  Needed until cloudstorage_stub.py is updated to accept compose requests
  TODO: When patched remove the True flow from this if.
  '''
  if 'development' in os.environ.get('SERVER_SOFTWARE', '').lower():
    def _temp_func(bucket, file_list, destination_file, content_type):
      '''
        Dev server stub, remove when the dev server accepts compose requests

      '''
      with open(destination_file, 'w', content_type=content_type) as gcs_merge:
        for source_file in file_list:
          try:
            with open(bucket + source_file['file_name'], 'r') as gcs_source:
              gcs_merge.write(gcs_source.read())
          except errors.NotFoundError:
            logging.warn('File not found %s, skipping', source_file['file_name'])
    compose_object = _temp_func
  else:
    compose_object = api.compose_object
  file_list, _, content_type = _validate_compose_list(
                                      destination_file,
                                      list_of_files, content_type, 32)
  compose_object(file_list, destination_file, content_type)

def compose_batch(list_of_files, destination_file,
            content_type=None, retry_params=None, _account_id=None):
  '''
  Runs the GCS Compose on the inputed files in batches of 32.
    Merges between 2 and 1024 files into one file. If any of the previous files had the merge
    run on them it counts towards the 1024 limit.
    Automatically breaks down the files into batches of 32.
    This method is slower and could result in orphan files if it fails
  Args:
    list_of_files: list of file name strings or dictionaries with the following format:
      {'file_name': REQUIRED name of the file to be merged. Do not include the bucket name,
       'Generation': OPTIONAL Used to specify what version of a file to use,
       'IfGenerationMatch': OPTIONAL Used to fail requests if versions don't match}
    destination_file: Path to the desired output file. Must have the bucket in the path.
    preserve_order: If true will not sort the files into natural order.
    content_type: Used to specify the content-header of the output.
                  If None will try to guess based off the first file.
    retry_params: An api_utils.RetryParams for this call to GCS. If None,
    the default one is used.
    _account_id: Internal-use only.

  Raises:
    TypeError: If the dictionary for the file list is malformed
    ValueError: If the number of files is outside the range of 2-1024
    errors.NotFoundError: If any element in the file list is missing the 'file_name' key
  '''

  temp_file_suffix = '____MergeTempFile'

  file_list, bucket, content_type = _validate_compose_list(
                                      destination_file, list_of_files,
                                   content_type, number_of_files=1024)

  '''
  Compose can only handle 32 files at a time. Breaks down the list into batches of 32
  (this will only need to happen once, since the file_list size restriction is 1024 = 32 * 32)
  '''
  temp_list = []  # temporary storage for the filenames that store the merged segments of 32
  if len(file_list) > 32:
    temp_file_counter = 0
    segments_list = [file_list[i:i + 32] for i in range(0, len(file_list), 32)]
    file_list = []
    for segment in segments_list:
      if len(segment) > 1:
        temp_file_name = destination_file + temp_file_suffix + str(temp_file_counter)
        compose(segment, temp_file_name, content_type, retry_params, _account_id)
        file_list.append({'file_name': temp_file_name.replace(bucket, '', 1)})
        temp_file_counter += 1
        temp_list.append(temp_file_name)
      else:
        file_list.append(segment[0])
  # There will always be 32 or less files to merge at this point
  compose(file_list, destination_file, content_type, retry_params, _account_id)
  # grab all temp files that were created during the merging of segments of 32
  temp_list = listbucket(destination_file + temp_file_suffix)
  # delete all the now-unneeded temporary merge-files for the segments of 32 (if any)
  for item in temp_list:
    try:
      delete(item.filename)
    except errors.NotFoundError:
      pass

def file_exists(destination):
  '''Checks if a file exists.
    Tries to do a stat check on the file.
    If it succeeds returns True otherwise false

    Args:
      destination: Full path to the file (ie. /bucket/object) w/ leading slash
    Returns:
      True if the file is accessible otherwise False
    '''
  try:
    stat(destination)
    return True
  except errors.NotFoundError:
    return False

def _validate_compose_list(destination_file, file_list, content_type, number_of_files=32):
  '''
    Validates the compose list and extracts needed information from it
    Args:
      destination: Full path to the file (ie. /bucket/object),
      file_list: list of files to compose, see compose for details,
      content_type: content type specified by the user,
      number_of_files=32: maximum number of files allowed in the list
    Returns:
      list_of_files: Ready to use dict version of the list,
      bucket: bucket name extracted from the file paths,
      content_type: either the specified content_type or one devined from the first file
  '''
  common.validate_file_path(destination_file)
  bucket = '/' + destination_file.split('/')[1] + '/'
  if not isinstance(file_list, list):
    raise TypeError('file_list must be a list of strings or dictionaries')
  list_len = len(file_list)
  if list_len > number_of_files:
    raise ValueError(
          'Compose attempted to create composite with too many'
           '(%i) components; limit is (%i).' % (list_len, number_of_files))
  if list_len <= 1:
    raise ValueError('Compose operation requires at least two components; %i provided.' % list_len)

  if isinstance(file_list[0], str):
    list_of_files = [{"file_name" : file_name.replace(bucket, "")}
                   for file_name in file_list]
  else:
    list_of_files = file_list[:]
  for source_file in list_of_files:
    if not isinstance(source_file, dict):
      raise TypeError('Each item of file_list must be a string or dictionary')
    file_name = source_file.get('file_name', None)
    if file_name is None:
      raise errors.NotFoundError('Each item in file_list must specify a file_name')
    if file_name.startswith(bucket):
      logging.warn('Detected bucket name at the start of the file, '
                   'must not specify the bucket when listing file_names.'
                   ' May cause files to be miss read')
    common.validate_file_path(bucket + source_file['file_name'])
  if content_type is None:
    if file_exists(bucket + list_of_files[0]['file_name']):
      content_type = stat(bucket + list_of_files[0]['file_name']).content_type
    else:
      logging.warn('Unable to read first file to divine content type, using text/plain')
      content_type = 'text/plain'
  return list_of_files, bucket, content_type
  

class _Bucket(object):
  """A wrapper for a GCS bucket as the return value of listbucket."""

  def __init__(self, api, path, options):
    """Initialize.

    Args:
      api: storage_api instance.
      path: bucket path of form '/bucket'.
      options: a dict of listbucket options. Please see listbucket doc.
    """
    self._init(api, path, options)

  def _init(self, api, path, options):
    self._api = api
    self._path = path
    self._options = options.copy()
    self._get_bucket_fut = self._api.get_bucket_async(
        self._path + '?' + urllib.urlencode(self._options))
    self._last_yield = None
    self._new_max_keys = self._options.get('max-keys')

  def __getstate__(self):
    options = self._options
    if self._last_yield:
      options['marker'] = self._last_yield.filename[len(self._path) + 1:]
    if self._new_max_keys is not None:
      options['max-keys'] = self._new_max_keys
    return {'api': self._api,
            'path': self._path,
            'options': options}

  def __setstate__(self, state):
    self._init(state['api'], state['path'], state['options'])

  def __iter__(self):
    """Iter over the bucket.

    Yields:
      GCSFileStat: a GCSFileStat for an object in the bucket.
        They are ordered by GCSFileStat.filename.
    """
    total = 0
    max_keys = self._options.get('max-keys')

    while self._get_bucket_fut:
      status, resp_headers, content = self._get_bucket_fut.get_result()
      errors.check_status(status, [200], self._path, resp_headers=resp_headers,
                          body=content, extras=self._options)

      if self._should_get_another_batch(content):
        self._get_bucket_fut = self._api.get_bucket_async(
            self._path + '?' + urllib.urlencode(self._options))
      else:
        self._get_bucket_fut = None

      root = ET.fromstring(content)
      dirs = self._next_dir_gen(root)
      files = self._next_file_gen(root)
      next_file = files.next()
      next_dir = dirs.next()

      while ((max_keys is None or total < max_keys) and
             not (next_file is None and next_dir is None)):
        total += 1
        if next_file is None:
          self._last_yield = next_dir
          next_dir = dirs.next()
        elif next_dir is None:
          self._last_yield = next_file
          next_file = files.next()
        elif next_dir < next_file:
          self._last_yield = next_dir
          next_dir = dirs.next()
        elif next_file < next_dir:
          self._last_yield = next_file
          next_file = files.next()
        else:
          logging.error(
              'Should never reach. next file is %r. next dir is %r.',
              next_file, next_dir)
        if self._new_max_keys:
          self._new_max_keys -= 1
        yield self._last_yield

  def _next_file_gen(self, root):
    """Generator for next file element in the document.

    Args:
      root: root element of the XML tree.

    Yields:
      GCSFileStat for the next file.
    """
    for e in root.getiterator(common._T_CONTENTS):
      st_ctime, size, etag, key = None, None, None, None
      for child in e.getiterator('*'):
        if child.tag == common._T_LAST_MODIFIED:
          st_ctime = common.dt_str_to_posix(child.text)
        elif child.tag == common._T_ETAG:
          etag = child.text
        elif child.tag == common._T_SIZE:
          size = child.text
        elif child.tag == common._T_KEY:
          key = child.text
      yield common.GCSFileStat(self._path + '/' + key,
                               size, etag, st_ctime)
      e.clear()
    yield None

  def _next_dir_gen(self, root):
    """Generator for next directory element in the document.

    Args:
      root: root element in the XML tree.

    Yields:
      GCSFileStat for the next directory.
    """
    for e in root.getiterator(common._T_COMMON_PREFIXES):
      yield common.GCSFileStat(
          self._path + '/' + e.find(common._T_PREFIX).text,
          st_size=None, etag=None, st_ctime=None, is_dir=True)
      e.clear()
    yield None

  def _should_get_another_batch(self, content):
    """Whether to issue another GET bucket call.

    Args:
      content: response XML.

    Returns:
      True if should, also update self._options for the next request.
      False otherwise.
    """
    if ('max-keys' in self._options and
        self._options['max-keys'] <= common._MAX_GET_BUCKET_RESULT):
      return False

    elements = self._find_elements(
        content, set([common._T_IS_TRUNCATED,
                      common._T_NEXT_MARKER]))
    if elements.get(common._T_IS_TRUNCATED, 'false').lower() != 'true':
      return False

    next_marker = elements.get(common._T_NEXT_MARKER)
    if next_marker is None:
      self._options.pop('marker', None)
      return False
    self._options['marker'] = next_marker
    return True

  def _find_elements(self, result, elements):
    """Find interesting elements from XML.

    This function tries to only look for specified elements
    without parsing the entire XML. The specified elements is better
    located near the beginning.

    Args:
      result: response XML.
      elements: a set of interesting element tags.

    Returns:
      A dict from element tag to element value.
    """
    element_mapping = {}
    result = StringIO.StringIO(result)
    for _, e in ET.iterparse(result, events=('end',)):
      if not elements:
        break
      if e.tag in elements:
        element_mapping[e.tag] = e.text
        elements.remove(e.tag)
    return element_mapping
