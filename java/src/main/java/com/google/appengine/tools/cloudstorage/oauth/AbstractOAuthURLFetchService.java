/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.cloudstorage.oauth;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.tools.cloudstorage.ExceptionHandler;
import com.google.appengine.tools.cloudstorage.RetryHelper;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.appengine.tools.cloudstorage.RetryParams;
import com.google.apphosting.api.ApiProxy.ApiDeadlineExceededException;
import com.google.apphosting.api.ApiProxy.RPCFailedException;
import com.google.apphosting.api.ApiProxy.UnknownException;
import com.google.common.util.concurrent.Futures;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 * An OAuthURLFetchService decorator that adds the authorization header to the http request.
 *
 */
abstract class AbstractOAuthURLFetchService implements OAuthURLFetchService {

  private final URLFetchService urlFetch;

  private static final ExceptionHandler EXCEPTION_HANDLER = new ExceptionHandler.Builder().retryOn(
      UnknownException.class, RPCFailedException.class, ApiDeadlineExceededException.class).build();

  private static final RetryParams RETRY_PARAMS =
      new RetryParams.Builder().initialRetryDelayMillis(10).totalRetryPeriodMillis(10000).build();

  AbstractOAuthURLFetchService(URLFetchService urlFetch) {
    this.urlFetch = checkNotNull(urlFetch);
  }

  protected abstract String getToken();

  private HTTPRequest createAuthorizeRequest(final HTTPRequest req) throws RetryHelperException {
    String token = RetryHelper.runWithRetries(new Callable<String>() {
      @Override
      public String call() {
        return getToken();
      }
    }, RETRY_PARAMS, EXCEPTION_HANDLER);
    HTTPRequest request = URLFetchUtils.copyRequest(req);
    request.setHeader(new HTTPHeader("Authorization", "Bearer " + token));
    return request;
  }

  @Override
  public HTTPResponse fetch(HTTPRequest req) throws IOException, RetryHelperException {
    HTTPRequest authorizedRequest = createAuthorizeRequest(req);
    return urlFetch.fetch(authorizedRequest);
  }

  @Override
  public Future<HTTPResponse> fetchAsync(HTTPRequest req) {
    HTTPRequest authorizedRequest;
    try {
      authorizedRequest = createAuthorizeRequest(req);
    } catch (RetryHelperException e) {
      return Futures.immediateFailedCheckedFuture(e);
    }
    return urlFetch.fetchAsync(authorizedRequest);
  }
}
