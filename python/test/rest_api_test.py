# Copyright 2012 Google Inc. All Rights Reserved.




import httplib
import pickle
import unittest
import mock

from google.appengine.ext import ndb
from google.appengine.api import app_identity
from google.appengine.api import memcache
from google.appengine.api import urlfetch
from google.appengine.ext import testbed

try:
  from cloudstorage import api_utils
  from cloudstorage import rest_api
  from cloudstorage import test_utils
except ImportError:
  from google.appengine.ext.cloudstorage import api_utils
  from google.appengine.ext.cloudstorage import rest_api
  from google.appengine.ext.cloudstorage import test_utils


class RestApiTest(unittest.TestCase):

  def setUp(self):
    super(RestApiTest, self).setUp()
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_app_identity_stub()
    self.testbed.init_datastore_v3_stub()
    self.testbed.init_memcache_stub()
    self.testbed.init_urlfetch_stub()
    api_utils._thread_local_settings.retry_params = None

  def tearDown(self):
    self.testbed.deactivate()
    super(RestApiTest, self).tearDown()

  def testBasicCall(self):
    api = rest_api._RestApi('scope')
    self.assertEqual(api.scopes, ['scope'])

    fut_get_token = ndb.Future()
    fut_get_token.set_result('blah')
    api.get_token_async = mock.create_autospec(api.get_token_async,
                                               return_value=fut_get_token)

    fut_urlfetch = ndb.Future()
    fut_urlfetch.set_result(
        test_utils.MockUrlFetchResult(200, {'foo': 'bar'}, 'yoohoo'))
    ctx_urlfetch = mock.Mock(return_value=fut_urlfetch)
    ndb.get_context().urlfetch = ctx_urlfetch

    res = api.do_request('http://example.com')

    self.assertEqual(res, (200, {'foo': 'bar'}, 'yoohoo'))
    ctx_urlfetch.assert_called_once_with(
        'http://example.com',
        headers={'authorization': 'OAuth blah',
                 'User-Agent': 'AppEngine-Python-GCS'},
        follow_redirects=False,
        payload=None,
        method='GET',
        deadline=None,
        callback=None)

  def testBasicCallWithUserAgent(self):
    user_agent = 'Test User Agent String'
    retry_params = api_utils.RetryParams(_user_agent=user_agent)
    api = rest_api._RestApi('scope', retry_params=retry_params)
    self.assertEqual(api.scopes, ['scope'])

    fut_get_token = ndb.Future()
    fut_get_token.set_result('blah')
    api.get_token_async = mock.create_autospec(api.get_token_async,
                                               return_value=fut_get_token)

    fut_urlfetch = ndb.Future()
    fut_urlfetch.set_result(
        test_utils.MockUrlFetchResult(200, {'foo': 'bar'}, 'yoohoo'))
    ctx_urlfetch = mock.Mock(return_value=fut_urlfetch)
    ndb.get_context().urlfetch = ctx_urlfetch

    res = api.do_request('http://example.com')

    self.assertEqual(res, (200, {'foo': 'bar'}, 'yoohoo'))
    ctx_urlfetch.assert_called_once_with(
        'http://example.com',
        headers={'authorization': 'OAuth blah',
                 'User-Agent': user_agent},
        follow_redirects=False,
        payload=None,
        method='GET',
        deadline=None,
        callback=None)

  def testNoToken(self):
    api = rest_api._RestApi('scope')
    self.assertEqual(api.scopes, ['scope'])

    fut_get_token = ndb.Future()
    fut_get_token.set_result(None)
    api.get_token_async = mock.create_autospec(api.get_token_async,
                                               return_value=fut_get_token)

    fut_urlfetch = ndb.Future()
    fut_urlfetch.set_result(
        test_utils.MockUrlFetchResult(200, {'foo': 'bar'}, 'yoohoo'))
    ctx_urlfetch = mock.Mock(return_value=fut_urlfetch)
    ndb.get_context().urlfetch = ctx_urlfetch

    res = api.do_request('http://example.com')

    self.assertEqual(res, (200, {'foo': 'bar'}, 'yoohoo'))
    ctx_urlfetch.assert_called_once_with(
        'http://example.com',
        headers={'User-Agent': 'AppEngine-Python-GCS'},
        follow_redirects=False,
        payload=None,
        method='GET',
        deadline=None,
        callback=None)

  def testMultipleScopes(self):
    api = rest_api._RestApi(['scope1', 'scope2'])
    self.assertEqual(api.scopes, ['scope1', 'scope2'])

  def testNegativeTimeout(self):
    api = rest_api._RestApi('scope')
    fut1 = ndb.Future()
    fut1.set_result(('token1', 0))
    fut2 = ndb.Future()
    fut2.set_result(('token2', 0))
    api.make_token_async = mock.create_autospec(
        api.make_token_async, side_effect=[fut1, fut2])
    token1 = api.get_token()
    token2 = api.get_token()
    self.assertNotEqual(token1, token2)

  def testNoExpiredToken(self):
    with mock.patch('time.time') as t:
      t.side_effect = [2, 4, 5, 6]
      api = rest_api._RestApi('scope')
      fut1 = ndb.Future()
      fut1.set_result(('token1', 3 + api.expiration_headroom))
      fut2 = ndb.Future()
      fut2.set_result(('token2', 7 + api.expiration_headroom))
      api.make_token_async = mock.create_autospec(
          api.make_token_async, side_effect=[fut1, fut2])

      token = api.get_token()
      self.assertEqual('token1', token)
      token = api.get_token()
      self.assertEqual('token2', token)
      token = api.get_token()
      self.assertEqual('token2', token)

  def testTokenMemoized(self):

    ndb_ctx = ndb.get_context()
    ndb_ctx.set_cache_policy(lambda key: False)
    ndb_ctx.set_memcache_policy(lambda key: False)

    api = rest_api._RestApi('scope')
    t1 = api.get_token()
    self.assertNotEqual(None, t1)

    api = rest_api._RestApi('scope')
    t2 = api.get_token()
    self.assertEqual(t2, t1)

  def testTokenSaved(self):
    retry_params = api_utils.RetryParams(save_access_token=True)
    api = rest_api._RestApi('scope', retry_params=retry_params)
    t1 = api.get_token()
    self.assertNotEqual(None, t1)

    api = rest_api._RestApi('scope', retry_params=retry_params)
    t2 = api.get_token()
    self.assertEqual(t2, t1)

    memcache.flush_all()
    ndb.get_context().clear_cache()

    api = rest_api._RestApi('scope', retry_params=retry_params)
    t3 = api.get_token()
    self.assertEqual(t3, t1)

  def testDifferentServiceAccounts(self):
    api1 = rest_api._RestApi('scope', 123)
    api2 = rest_api._RestApi('scope', 456)

    t1 = api1.get_token()
    t2 = api2.get_token()
    self.assertNotEqual(t1, t2)

  def testSameServiceAccount(self):
    api1 = rest_api._RestApi('scope', 123)
    api2 = rest_api._RestApi('scope', 123)

    t1 = api1.get_token()
    t2 = api2.get_token()
    self.assertEqual(t1, t2)

  def testCallUrlFetch(self):
    api = rest_api._RestApi('scope')

    fut = ndb.Future()
    fut.set_result(test_utils.MockUrlFetchResult(200, {}, 'response'))
    ndb.Context.urlfetch = mock.create_autospec(
        ndb.Context.urlfetch,
        return_value=fut)

    res = api.urlfetch('http://example.com', method='PUT', headers={'a': 'b'})

    self.assertEqual(res.status_code, 200)
    self.assertEqual(res.content, 'response')

  def testPickling(self):
    retry_params = api_utils.RetryParams(max_retries=1000)
    api = rest_api._RestApi('scope', service_account_id=1,
                            retry_params=retry_params)
    self.assertNotEqual(None, api.get_token())

    pickled_api = pickle.loads(pickle.dumps(api))
    self.assertEqual(0, len(set(api.__dict__.keys()) ^
                            set(pickled_api.__dict__.keys())))
    for k, v in api.__dict__.iteritems():
      if not hasattr(v, '__call__'):
        self.assertEqual(v, pickled_api.__dict__[k])

    pickled_api.token = None

    fut_urlfetch = ndb.Future()
    fut_urlfetch.set_result(
        test_utils.MockUrlFetchResult(200, {'foo': 'bar'}, 'yoohoo'))
    pickled_api.urlfetch_async = mock.create_autospec(
        pickled_api.urlfetch_async, return_value=fut_urlfetch)

    res = pickled_api.do_request('http://example.com')
    self.assertEqual(res, (200, {'foo': 'bar'}, 'yoohoo'))

  def testUrlFetchCalledWithUserProvidedDeadline(self):
    retry_params = api_utils.RetryParams(urlfetch_timeout=90)
    api = rest_api._RestApi('scope', retry_params=retry_params)

    resp_fut1 = ndb.Future()
    resp_fut1.set_exception(urlfetch.DownloadError())
    resp_fut2 = ndb.Future()
    resp_fut2.set_result(test_utils.MockUrlFetchResult(httplib.ACCEPTED,
                                                       None, None))
    ndb.Context.urlfetch = mock.create_autospec(
        ndb.Context.urlfetch,
        side_effect=[resp_fut1, resp_fut2])

    self.assertEqual(httplib.ACCEPTED, api.do_request('foo')[0])
    self.assertEqual(
        90, ndb.Context.urlfetch.call_args_list[0][1]['deadline'])
    self.assertEqual(
        90, ndb.Context.urlfetch.call_args_list[1][1]['deadline'])

  def testRetryAfterDoRequestUrlFetchTimeout(self):
    api = rest_api._RestApi('scope')

    resp_fut1 = ndb.Future()
    resp_fut1.set_exception(urlfetch.DownloadError())
    resp_fut2 = ndb.Future()
    resp_fut2.set_result(test_utils.MockUrlFetchResult(httplib.ACCEPTED,
                                                       None, None))
    ndb.Context.urlfetch = mock.create_autospec(
        ndb.Context.urlfetch,
        side_effect=[resp_fut1, resp_fut2])

    self.assertEqual(httplib.ACCEPTED, api.do_request('foo')[0])
    self.assertEqual(2, ndb.Context.urlfetch.call_count)

  def testRetryAfterDoRequestResponseTimeout(self):
    api = rest_api._RestApi('scope')

    resp_fut1 = ndb.Future()
    resp_fut1.set_result(test_utils.MockUrlFetchResult(httplib.REQUEST_TIMEOUT,
                                                       None, None))
    resp_fut2 = ndb.Future()
    resp_fut2.set_result(test_utils.MockUrlFetchResult(httplib.ACCEPTED,
                                                       None, None))
    ndb.Context.urlfetch = mock.create_autospec(
        ndb.Context.urlfetch,
        side_effect=[resp_fut1, resp_fut2])

    self.assertEqual(httplib.ACCEPTED, api.do_request('foo')[0])
    self.assertEqual(2, ndb.Context.urlfetch.call_count)

  def testRetryAfterAppIdentityError(self):
    api = rest_api._RestApi('scope')

    token_fut = ndb.Future()
    token_fut.set_result('token1')
    api.get_token_async = mock.create_autospec(
        api.get_token_async,
        side_effect=[app_identity.InternalError,
                     app_identity.InternalError,
                     token_fut])

    resp_fut = ndb.Future()
    resp_fut.set_result(test_utils.MockUrlFetchResult(httplib.ACCEPTED,
                                                      None, None))
    ndb.Context.urlfetch = mock.create_autospec(
        ndb.Context.urlfetch,
        side_effect=[resp_fut])

    self.assertEqual(httplib.ACCEPTED, api.do_request('foo')[0])
    self.assertEqual(
        'OAuth token1',
        ndb.Context.urlfetch.call_args[1]['headers']['authorization'])
    self.assertEqual(3, api.get_token_async.call_count)


if __name__ == '__main__':
  unittest.main()
