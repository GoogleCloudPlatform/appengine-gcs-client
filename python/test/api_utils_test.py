# Copyright 2013 Google Inc. All Rights Reserved.

"""Tests for api_utils.py."""

import os
import threading
import time
import unittest

import mock

from google.appengine.ext import ndb
from google.appengine import runtime
from google.appengine.api import apiproxy_stub_map
from google.appengine.ext import testbed

try:
  from cloudstorage import api_utils
  from google.appengine.api import app_identity
  from google.appengine.api import urlfetch
  from google.appengine.api import urlfetch_errors
  from google.appengine.runtime import apiproxy_errors
except ImportError:
  from google.appengine.ext.cloudstorage import api_utils
  from google.appengine.api import app_identity
  from google.appengine.api import urlfetch
  from google.appengine.api import urlfetch_errors
  from google.appengine.runtime import apiproxy_errors




class EagerTaskletTest(unittest.TestCase):
  """Tests for eager tasklet decorator."""

  def setUp(self):
    self.testbed = testbed.Testbed()
    self.testbed.activate()
    self.testbed.init_all_stubs()
    self.urlfetch_called = False
    hooks = apiproxy_stub_map.apiproxy.GetPreCallHooks()
    hooks.Append('key', self.UrlfetchPreCallHook, 'urlfetch')

  def tearDown(self):
    self.testbed.deactivate()

  def LazyTasklet(self):
    """A ndb tasklet that does urlfetch."""
    ctx = ndb.get_context()
    return ctx.urlfetch('http://www.google.com')

  @api_utils._eager_tasklet
  def EagerTasklet(self):
    """Same tasklet but with decorator."""
    ctx = ndb.get_context()
    return ctx.urlfetch('http://www.google.com')

  def UrlfetchPreCallHook(self, service, call, req, res, rpc, error):
    if service == 'urlfetch':
      self.urlfetch_called = True

  def testLazyTasklet(self):
    fut = self.LazyTasklet()
    self.assertFalse(self.urlfetch_called)
    try:
      fut.get_result()
    except:
      pass
    self.assertTrue(self.urlfetch_called)

  def testEagerTasklet(self):
    self.assertFalse(self.urlfetch_called)
    self.EagerTasklet()
    self.assertTrue(self.urlfetch_called)


class FilenameEscapingTest(unittest.TestCase):
  """Tests for _quote_filename and _unquote_filename."""

  def testEscaping(self):
    def EscapeUnescapeFilename(unescaped, escaped):
      self.assertEqual(escaped, api_utils._quote_filename(unescaped))
      self.assertEqual(unescaped, api_utils._unquote_filename(escaped))

    EscapeUnescapeFilename('/bucket/foo', '/bucket/foo')
    EscapeUnescapeFilename('/bucket._-bucket/foo', '/bucket._-bucket/foo')
    EscapeUnescapeFilename('/bucket/a ;/?:@&=+$,',
                           '/bucket/a%20%3B/%3F%3A%40%26%3D%2B%24%2C')


class RetryParamsTest(unittest.TestCase):
  """Tests for RetryParams."""

  def testInitialization(self):
    retry_params = api_utils._get_default_retry_params()
    self.assertEqual('AppEngine-Python-GCS', retry_params._user_agent)

    user_agent = 'Test User Agent String'
    retry_params = api_utils.RetryParams(_user_agent=user_agent)
    self.assertEqual(user_agent, retry_params._user_agent)

  def testValidation(self):
    self.assertRaises(TypeError, api_utils.RetryParams, 2)
    self.assertRaises(TypeError, api_utils.RetryParams, urlfetch_timeout='foo')
    self.assertRaises(TypeError, api_utils.RetryParams, max_retries=1.1)
    self.assertRaises(ValueError, api_utils.RetryParams, initial_delay=0)
    self.assertRaises(TypeError, api_utils.RetryParams, save_access_token='')
    api_utils.RetryParams(backoff_factor=1)
    api_utils.RetryParams(save_access_token=True)

  def testNoDelay(self):
    start_time = time.time()
    retry_params = api_utils.RetryParams(max_retries=0, min_retries=5)
    self.assertEqual(-1, retry_params.delay(1, start_time))
    retry_params = api_utils.RetryParams(max_retry_period=1, max_retries=1)
    self.assertEqual(-1, retry_params.delay(2, start_time - 2))

  def testMinRetries(self):
    start_time = time.time()
    retry_params = api_utils.RetryParams(min_retries=3,
                                         max_retry_period=10,
                                         initial_delay=1)
    with mock.patch('time.time') as t:
      t.return_value = start_time + 11
      self.assertEqual(1, retry_params.delay(1, start_time))

  def testPerThreadSetting(self):
    set_count = [0]
    cv = threading.Condition()

    retry_params1 = api_utils.RetryParams(max_retries=1000)
    retry_params2 = api_utils.RetryParams(max_retries=2000)
    retry_params3 = api_utils.RetryParams(max_retries=3000)

    def Target(retry_params):
      api_utils.set_default_retry_params(retry_params)
      with cv:
        set_count[0] += 1
        if set_count[0] != 3:
          cv.wait()
        cv.notify()
      self.assertEqual(retry_params, api_utils._get_default_retry_params())

    threading.Thread(target=Target, args=(retry_params1,)).start()
    threading.Thread(target=Target, args=(retry_params2,)).start()
    threading.Thread(target=Target, args=(retry_params3,)).start()

  def testPerRequestSetting(self):
    os.environ['REQUEST_LOG_ID'] = '1'
    retry_params = api_utils.RetryParams(max_retries=1000)
    api_utils.set_default_retry_params(retry_params)
    self.assertEqual(retry_params, api_utils._get_default_retry_params())

    os.environ['REQUEST_LOG_ID'] = '2'
    self.assertEqual(api_utils.RetryParams(),
                     api_utils._get_default_retry_params())

  def testDelay(self):
    start_time = time.time()
    retry_params = api_utils.RetryParams(backoff_factor=3,
                                         initial_delay=1,
                                         max_delay=28,
                                         max_retries=10,
                                         max_retry_period=100)
    with mock.patch('time.time') as t:
      t.return_value = start_time + 1
      self.assertEqual(1, retry_params.delay(1, start_time))
      self.assertEqual(3, retry_params.delay(2, start_time))
      self.assertEqual(9, retry_params.delay(3, start_time))
      self.assertEqual(27, retry_params.delay(4, start_time))
      self.assertEqual(28, retry_params.delay(5, start_time))
      self.assertEqual(28, retry_params.delay(6, start_time))
      t.return_value = start_time + 101
      self.assertEqual(-1, retry_params.delay(7, start_time))


@ndb.tasklet
def test_tasklet1(a, b):
  result = yield test_tasklet2(a)
  raise ndb.Return(result, b)


@ndb.tasklet
def test_tasklet2(a):
  raise ndb.Return(a)


@ndb.tasklet
def test_tasklet3(a):
  raise ValueError('Raise an error %r for testing.' % a)


@ndb.tasklet
def test_tasklet4():
  raise runtime.DeadlineExceededError('Raise an error for testing.')


class RetriableTaskletTest(unittest.TestCase):
  """Tests for _retriable_tasklet."""

  def setUp(self):
    super(RetriableTaskletTest, self).setUp()
    self.invoked = 0

  @ndb.tasklet
  def tasklet_for_test(self, results):
    r = results[self.invoked]
    self.invoked += 1
    if isinstance(r, type) and issubclass(r, Exception):
      raise r('Raise an error for testing for the %d time.' % self.invoked)
    raise ndb.Return(r)

  def testTaskletWasSuccessful(self):
    fut = api_utils._RetryWrapper(api_utils.RetryParams()).run(
        test_tasklet1, a=1, b=2)
    a, b = fut.get_result()
    self.assertEqual(1, a)
    self.assertEqual(2, b)

  def testRetryDueToBadResult(self):
    fut = api_utils._RetryWrapper(
        api_utils.RetryParams(min_retries=1, max_retries=3),
        should_retry=lambda r: r < 0).run(
            self.tasklet_for_test, results=[-1, -1, 1])
    r = fut.get_result()
    self.assertEqual(1, r)

  def testRetryReturnedABadResult(self):
    fut = api_utils._RetryWrapper(
        api_utils.RetryParams(min_retries=1, max_retries=3),
        should_retry=lambda r: r < 0).run(
            self.tasklet_for_test, results=[-1, -1, -1, -1])
    r = fut.get_result()
    self.assertEqual(-1, r)

  def testRetryDueToTransientError(self):
    results = [urlfetch.DownloadError, apiproxy_errors.Error,
               app_identity.InternalError, app_identity.BackendDeadlineExceeded,
               urlfetch_errors.InternalTransientError, 1]

    fut = api_utils._RetryWrapper(
        api_utils.RetryParams(min_retries=1, max_retries=len(results)),
        retriable_exceptions=api_utils._RETRIABLE_EXCEPTIONS).run(
            self.tasklet_for_test,
            results=results)
    r = fut.get_result()
    self.assertEqual(1, r)

  def testRetryDueToError(self):
    fut = api_utils._RetryWrapper(
        api_utils.RetryParams(min_retries=1, max_retries=3),
        retriable_exceptions=(ValueError,)).run(
            self.tasklet_for_test,
            results=[ValueError, ValueError, ValueError, 1])
    r = fut.get_result()
    self.assertEqual(1, r)

  def testTooLittleRetry(self):
    fut = api_utils._RetryWrapper(
        api_utils.RetryParams(min_retries=0, max_retries=1),
        retriable_exceptions=(ValueError,)).run(
            self.tasklet_for_test,
            results=[ValueError, ValueError])
    self.assertRaises(ValueError, fut.get_result)

  def testNoRetryDueToRetryParams(self):
    retry_params = api_utils.RetryParams(min_retries=0, max_retries=0)
    fut = api_utils._RetryWrapper(
        retry_params, retriable_exceptions=[ValueError]).run(
            test_tasklet3, a=1)
    self.assertRaises(ValueError, fut.get_result)

  def testNoRetryDueToErrorType(self):
    fut = api_utils._RetryWrapper(
        api_utils.RetryParams(),
        retriable_exceptions=[TypeError]).run(
            test_tasklet3, a=1)
    self.assertRaises(ValueError, fut.get_result)

  def testRuntimeError(self):
    fut = api_utils._RetryWrapper(
        api_utils.RetryParams()).run(test_tasklet4)
    self.assertRaises(runtime.DeadlineExceededError, fut.get_result)


if __name__ == '__main__':
  unittest.main()
