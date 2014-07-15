/*
 * Copyright 2014 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.tools.cloudstorage.oauth.URLFetchUtils.HTTPRequestInfo;
import com.google.common.base.Strings;

import java.io.IOException;

/**
 * Handles failed HTTP responses.
 */
class HttpErrorHandler {

  static IOException error(int responseCode, String description) throws IOException {
    switch (responseCode) {
      case 400:
        throw createException("Server replied with 400, probably bad request", description);
      case 401:
        throw createException("Server replied with 401, probably bad authentication", description);
      case 403:
        throw createException(
            "Server replied with 403, verify ACLs are set correctly on the object and bucket",
            description);
      case 404:
        throw createException("Server replied with 404, probably no such bucket", description);
      case 412:
        throw createException("Server replied with 412, precondition failure", description);
      default:
        if (responseCode >= 500 && responseCode < 600) {
          throw new IOException("Response code " + responseCode + ", retryable: " + description);
        } else {
          throw createException("Unexpected response code " + responseCode, description);
        }
    }
  }

  static RuntimeException createException(String msg, String description) {
    if (Strings.isNullOrEmpty(description)) {
      return new RuntimeException(msg);
    } else {
      return new RuntimeException(msg + ": " + description);
    }
  }

  public static IOException error(HTTPRequestInfo req, HTTPResponse resp) throws IOException {
    return error(resp.getResponseCode(), URLFetchUtils.describeRequestAndResponse(req, resp));
  }
}
