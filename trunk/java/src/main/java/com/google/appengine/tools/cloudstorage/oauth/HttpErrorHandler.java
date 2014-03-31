/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponseException;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.common.base.Strings;

import java.io.IOException;

/**
 * Handles failed HTTP responses.
 */
abstract class HttpErrorHandler {

  private static class ApiClientHttpErrorHandler extends HttpErrorHandler {

    private final HttpRequest request;
    private final HttpResponseException exception;

    private ApiClientHttpErrorHandler(HttpRequest request, HttpResponseException exception) {
      this.request = request;
      this.exception = exception;
    }

    @Override
    protected int getResponseCode() {
      return exception.getStatusCode();
    }

    @Override
    protected String describeRequestAndResponse() {
      return URLFetchUtils.describeRequestAndResponse(request, exception);
    }
  }

  private static class AppEngineHttpErrorHandler extends HttpErrorHandler {

    private final HTTPRequest request;
    private final HTTPResponse response;

    private AppEngineHttpErrorHandler(HTTPRequest request, HTTPResponse response) {
      this.request = request;
      this.response = response;
    }

    @Override
    protected int getResponseCode() {
      return response.getResponseCode();
    }

    @Override
    protected String describeRequestAndResponse() {
      return URLFetchUtils.describeRequestAndResponse(request, response);
    }
  }

  private void error() throws IOException {
    int responseCode = getResponseCode();
    switch (responseCode) {
      case 400:
        throw createException("Server replied with 400, probably bad request");
      case 401:
        throw createException("Server replied with 401, probably bad authentication");
      case 403:
        throw createException(
            "Server replied with 403, verify ACLs are set correctly on the object and bucket");
      case 404:
        throw createException("Server replied with 404, probably no such bucket");
      case 412:
        throw createException("Server replied with 412, precondition failure");
      default:
        if (responseCode >= 500 && responseCode < 600) {
          throw new IOException("Response code " + responseCode + ", retryable: "
              + describeRequestAndResponse());
        } else {
          throw createException("Unexpected response code " + getResponseCode());
        }
    }
  }

  private RuntimeException createException(String msg) {
    String info = describeRequestAndResponse();
    if (!Strings.isNullOrEmpty(info)) {
      msg += ": " + info;
    }
    return new RuntimeException(msg);
  }

  protected abstract int getResponseCode();

  protected abstract String describeRequestAndResponse();

  static IOException error(HttpRequest request, HttpResponseException exception)
      throws IOException {
    HttpErrorHandler handler = new HttpErrorHandler.ApiClientHttpErrorHandler(request, exception);
    handler.error();
    return null;
  }

  static IOException error(HTTPRequest request, HTTPResponse response) throws IOException {
    HttpErrorHandler handler = new HttpErrorHandler.AppEngineHttpErrorHandler(request, response);
    handler.error();
    return null;
  }
}