# Copyright 2012 Google Inc. All Rights Reserved.

"""Base and helper classes for Google RESTful APIs."""





__all__ = ['add_sync_methods']

import logging

from . import common
from . import stub_dispatcher


try:
  from google.appengine.api import app_identity
  from google.appengine.ext import ndb
except ImportError:
  from google.appengine.api import app_identity
  from google.appengine.ext import ndb


def _make_sync_method(name):
  """Helper to synthesize a synchronous method from an async method name.

  Used by the @add_sync_methods class decorator below.

  Args:
    name: The name of the synchronous method.

  Returns:
    A method (with first argument 'self') that retrieves and calls
    self.<name>, passing its own arguments, expects it to return a
    Future, and then waits for and returns that Future's result.
  """

  def sync_wrapper(self, *args, **kwds):
    method = getattr(self, name)
    future = method(*args, **kwds)
    return future.get_result()

  return sync_wrapper


def add_sync_methods(cls):
  """Class decorator to add synchronous methods corresponding to async methods.

  This modifies the class in place, adding additional methods to it.
  If a synchronous method of a given name already exists it is not
  replaced.

  Args:
    cls: A class.

  Returns:
    The same class, modified in place.
  """
  for name in cls.__dict__.keys():
    if name.endswith('_async'):
      sync_name = name[:-6]
      if not hasattr(cls, sync_name):
        setattr(cls, sync_name, _make_sync_method(name))
  return cls


class _AE_TokenStorage_(ndb.Model):
  """Entity to store app_identity tokens in memcache."""

  token = ndb.StringProperty()


class _RestApi(object):
  """Base class for REST-based API wrapper classes.

  This class manages authentication tokens and request retries.  All
  APIs are available as synchronous and async methods; synchronous
  methods are synthesized from async ones by the add_sync_methods()
  function in this module.

  WARNING: Do NOT directly use this api. It's an implementation detail
  and is subject to change at any release.
  """

  def __init__(self, scopes, service_account_id=None):
    """Constructor.

    Args:
      scopes: A scope or a list of scopes.
    """

    if isinstance(scopes, basestring):
      scopes = [scopes]
    self.scopes = scopes
    self.service_account_id = service_account_id
    self.token = None

  @ndb.tasklet
  def do_request_async(self, url, method='GET', headers=None, payload=None,
                       deadline=None, callback=None):
    """Issue one HTTP request.

    This is a thin (async) wrapper around urlfetch() which adds an
    authentication header and retries on a 401 status code.  It also
    logs requests and responses
    """
    headers = {} if headers is None else dict(headers)
    token = yield self.get_token_async()
    headers['authorization'] = 'OAuth ' + token
    if url.startswith(common.LOCAL_API_URL):
      resp = stub_dispatcher.dispatch(method, headers, url, payload)
    else:
      resp = yield self.urlfetch_async(url, payload=payload, method=method,
                                       headers=headers, follow_redirects=False,
                                       deadline=deadline, callback=callback)

      if resp.status_code == 401:
        token = yield self.get_token_async(refresh=True)
        headers['authorization'] = 'OAuth ' + token
        resp = yield self.urlfetch_async(url, payload=payload, method=method,
                                         headers=headers,
                                         follow_redirects=False,
                                         deadline=deadline, callback=callback)
    if resp.status_code >= 400:
      logging.debug('rest call error: status=%r\nheaders=%r\nbody=%.1000s',
                    resp.status_code, resp.headers, resp.content)
    raise ndb.Return((resp.status_code, resp.headers, resp.content))

  @ndb.tasklet
  def get_token_async(self, refresh=False):
    """Get an authentication token.

    The token is cached in memcache, keyed by the scopes argument.

    Args:
      refresh: If True, ignore a cached token; default False.

    Returns:
      An authentication token.
    """
    if self.token is not None and not refresh:
      raise ndb.Return(self.token)
    key = '%s,%s' % (self.service_account_id, ','.join(self.scopes))
    ts = None
    if not refresh:
      ts = yield _AE_TokenStorage_.get_by_id_async(key, use_datastore=False)
    if ts is None:
      rpc = app_identity.create_rpc()
      app_identity.make_get_access_token_call(rpc, self.scopes,
                                              self.service_account_id)
      token, expires_at = yield rpc
      ts = _AE_TokenStorage_(id=key, token=token)
      yield ts.put_async(memcache_timeout=expires_at - 300, use_datastore=False)
    self.token = ts.token
    raise ndb.Return(self.token)

  def urlfetch_async(self, url, **kwds):
    """Make an async urlfetch() call.

    This just passes the url and keyword arguments to NDB's async
    urlfetch() wrapper in the current context.

    This returns a Future despite not being decorated with @ndb.tasklet!
    """
    ctx = ndb.get_context()
    return ctx.urlfetch(url, **kwds)


_RestApi = add_sync_methods(_RestApi)
