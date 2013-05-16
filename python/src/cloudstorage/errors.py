# Copyright 2012 Google Inc. All Rights Reserved.

"""Google Cloud Storage specific Files API calls."""





__all__ = ['AuthorizationError',
           'check_status',
           'Error',
           'FatalError',
           'ForbiddenError',
           'NotFoundError',
           'ServerError',
           'TimeoutError',
           'TransientError',
          ]

import httplib


class Error(Exception):
  """Base error for all gcs operations."""


class TransientError(Error):
  """TransientError could be retried."""


class TimeoutError(TransientError):
  """Http 408 timeout."""


class FatalError(Error):
  """FatalError shouldn't be retried."""


class NotFoundError(FatalError):
  """Http 404 resource not found."""


class ForbiddenError(FatalError):
  """Http 403 Forbidden."""


class AuthorizationError(FatalError):
  """Http 401 authentication required."""


class InvalidRange(FatalError):
  """Http 416 RequestRangeNotSatifiable."""


class ServerError(FatalError):
  """Http >= 500 server side error."""


def check_status(status, expected, headers=None):
  """Check HTTP response status is expected.

  Args:
    status: HTTP response status. int.
    expected: a list of expected statuses. A list of ints.
    headers: HTTP response headers.

  Raises:
    AuthorizationError: if authorization failed.
    NotFoundError: if an object that's expected to exist doesn't.
    TimeoutError: if HTTP request timed out.
    ServerError: if server experienced some errors.
    FatalError: if any other unexpected errors occurred.
  """
  if status in expected:
    return

  msg = ('Expect status %r from Google Storage. But got status %d. Response '
         'headers: %r' %
         (expected, status, headers))

  if status == httplib.UNAUTHORIZED:
    raise AuthorizationError(msg)
  elif status == httplib.FORBIDDEN:
    raise ForbiddenError(msg)
  elif status == httplib.NOT_FOUND:
    raise NotFoundError(msg)
  elif status == httplib.REQUEST_TIMEOUT:
    raise TimeoutError(msg)
  elif status == httplib.REQUESTED_RANGE_NOT_SATISFIABLE:
    raise InvalidRange(msg)
  elif status >= 500:
    raise ServerError(msg)
  else:
    raise FatalError(msg)
