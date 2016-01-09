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

import com.google.appengine.api.ThreadManager;
import com.google.appengine.api.urlfetch.FetchOptions;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.io.ByteStreams;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


/**
 * Factory for RawGcsService using OAuth for authorization.
 */
public final class OauthRawGcsServiceFactory {

  private static final AppIdentityOAuthURLFetchService appIdFetchService =
      new AppIdentityOAuthURLFetchService(getUrlFetchService(), OauthRawGcsService.OAUTH_SCOPES);

  private OauthRawGcsServiceFactory() {}

  /**
   * @param headers
   * @return a new RawGcsService
   */
  public static RawGcsService createOauthRawGcsService(ImmutableSet<HTTPHeader> headers) {
    return new OauthRawGcsService(appIdFetchService, headers);
  }

  private static URLFetchService getUrlFetchService() {
    if (Boolean.parseBoolean(System.getenv("GAE_VM"))) {
      return new URLConnectionAdapter();
    }
    return URLFetchServiceFactory.getURLFetchService();
  }

  private static class URLConnectionAdapter implements URLFetchService {

    private static final ExecutorService executor =
        new ThreadPoolExecutor(1, 100,
            0L, TimeUnit.MILLISECONDS,
            new SynchronousQueue<Runnable>(),
            ThreadManager.currentRequestThreadFactory(),
            new ThreadPoolExecutor.CallerRunsPolicy());

    @Override
    public HTTPResponse fetch(URL url) throws IOException {
      return createHttpResponse((HttpURLConnection) url.openConnection());
    }

    @Override
    public HTTPResponse fetch(HTTPRequest req) throws IOException {
      URL url = req.getURL();
      HttpURLConnection connection = (HttpURLConnection) url.openConnection();
      connection.setRequestMethod(req.getMethod().name());
      for (HTTPHeader header : req.getHeaders()) {
        connection.setRequestProperty(header.getName(), header.getValue());
      }
      FetchOptions fetchOptions = req.getFetchOptions();
      connection.setInstanceFollowRedirects(fetchOptions.getFollowRedirects());
      if (fetchOptions.getDeadline() != null) {
        int timeout = (int) (fetchOptions.getDeadline() * 1000);
        connection.setConnectTimeout(timeout);
        connection.setReadTimeout(timeout);
      } else {
        connection.setConnectTimeout(20_000);
        connection.setReadTimeout(30_000);
      }
      byte[] payload = req.getPayload();
      if (payload != null) {
        connection.setDoOutput(true);
        OutputStream wr = connection.getOutputStream();
        wr.write(payload);
        wr.flush();
        wr.close();
      }
      return createHttpResponse(connection);
    }

    private HTTPResponse createHttpResponse(HttpURLConnection connection) throws IOException {
      final int responseCode = connection.getResponseCode();
      final byte[] content =
          ByteStreams.toByteArray(new BufferedInputStream(connection.getInputStream()));
      List<HTTPHeader> headers =
          Lists.newArrayListWithCapacity(connection.getHeaderFields().size());
      for (Map.Entry<String, List<String>> h : connection.getHeaderFields().entrySet()) {
        for (String v : h.getValue()) {
          headers.add(new HTTPHeader(h.getKey(), v));
        }
      }
      return new HTTPResponse(responseCode, content, null, headers);
    }

    @Override
    public Future<HTTPResponse> fetchAsync(final URL url) {
      return executor.submit(new Callable<HTTPResponse>() {
        @Override public HTTPResponse call() throws Exception {
          return fetch(url);
        }
      });
    }

    @Override
    public Future<HTTPResponse> fetchAsync(final HTTPRequest request) {
      return executor.submit(new Callable<HTTPResponse>() {
        @Override public HTTPResponse call() throws Exception {
          return fetch(request);
        }
      });
    }
  }
}
