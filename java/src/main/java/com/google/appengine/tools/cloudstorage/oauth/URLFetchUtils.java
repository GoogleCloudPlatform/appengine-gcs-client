/*
 * Copyright 2012 Google Inc. All Rights Reserved.
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

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.api.client.http.HttpContent;
import com.google.api.client.http.HttpHeaders;
import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpResponseException;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map.Entry;

/**
 * URLFetch-related utilities.
 */
final class URLFetchUtils {

  @VisibleForTesting
  static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'")
                    .withZoneUTC().withLocale(Locale.US);

  private URLFetchUtils() {}

  /**
   * This holds onto the relevant data for logging or error handling.
   * Holding onto an instance of this class rather than the request itself is useful because it
   * avoids holding a reference to the payload.
   */
  static class HTTPRequestInfo {

    private final String method;
    private final long length;
    private final URL url;
    private final HttpHeaders h1;
    private final List<HTTPHeader> h2;

    HTTPRequestInfo(HttpRequest req) {
      method = req.getRequestMethod();
      url = req.getUrl().toURL();
      long myLength;
      HttpContent content = req.getContent();
      try {
        myLength = content == null ? -1 : content.getLength();
      } catch (IOException e) {
        myLength = -1;
      }
      length = myLength;
      h1 = req.getHeaders();
      h2 = null;
    }

    HTTPRequestInfo(HTTPRequest req) {
      method = req.getMethod().toString();
      byte[] payload = req.getPayload();
      length = payload == null ? -1 : payload.length;
      url = req.getURL();
      h1 = null;
      h2 = req.getHeaders();
    }

    public void appendToString(StringBuilder b) {
      b.append(method).append(' ').append(url);
      if (h1 != null) {
        for (Entry<String, Object> h : h1.entrySet()) {
          b.append('\n').append(h.getKey()).append(": ").append(h.getValue());
        }
      }
      if (h2 != null) {
        for (HTTPHeader h : h2) {
          b.append('\n').append(h.getName()).append(": ").append(h.getValue());
        }
      }
      b.append("\n\n");
      if (length <= 0) {
        b.append("no content");
      } else {
        b.append(length).append(" bytes of content");
      }
      b.append('\n');
    }

    @Override
    public String toString() {
      StringBuilder buffer = new StringBuilder();
      appendToString(buffer);
      return buffer.toString();
    }
  }

  /**
   * Parses the date or returns null if it fails to do so.
   */
  static Date parseDate(String dateString) {
    try {
      return DATE_FORMAT.parseDateTime(dateString).toDate();
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  private static void appendResponse(HttpResponseException exception, StringBuilder b) {
    b.append(exception.getStatusCode()).append(" with ");
    Long contentLength = exception.getHeaders().getContentLength();
    b.append(contentLength != null ? contentLength : Long.valueOf(0));
    b.append(" bytes of content");
    HttpHeaders headers = exception.getHeaders();
    for (String name : headers.keySet()) {
      b.append('\n').append(name).append(": ").append(headers.get(name));
    }
    b.append('\n').append(exception.getContent()).append('\n');
  }

  static void appendResponse(HTTPResponse resp, StringBuilder b) {
    byte[] content = resp.getContent();
    b.append(resp.getResponseCode()).append(" with ").append(content == null ? 0 : content.length);
    b.append(" bytes of content");
    for (HTTPHeader h : resp.getHeadersUncombined()) {
      b.append('\n').append(h.getName()).append(": ").append(h.getValue());
    }
    b.append('\n').append(content == null ? "" : new String(content, UTF_8)).append('\n');
  }

  static String describeRequestAndResponse(HTTPRequestInfo req, HttpResponseException resp) {
    StringBuilder b = new StringBuilder(256).append("Request: ");
    req.appendToString(b);
    b.append("\nResponse: ");
    appendResponse(resp, b);
    return b.toString();
  }

  static String describeRequestAndResponse(HTTPRequestInfo req, HTTPResponse resp) {
    StringBuilder b = new StringBuilder(256).append("Request: ");
    req.appendToString(b);
    b.append("\nResponse: ");
    appendResponse(resp, b);
    return b.toString();
  }

  /** Gets all headers with the name {@code headerName}, case-insensitive. */
  private static Iterable<HTTPHeader> getHeaders(HTTPResponse resp, String headerName) {
    final String lowercaseHeaderName = headerName.toLowerCase();
    return Iterables.filter(resp.getHeadersUncombined(), new Predicate<HTTPHeader>() {
      @Override public boolean apply(HTTPHeader header) {
        return header.getName().toLowerCase().equals(lowercaseHeaderName);
      }
    });
  }

  /**
   * Checks that exactly one header named {@code headerName} is present and returns its value.
   */
  static String getSingleHeader(HTTPResponse resp, String headerName) {
    return Iterables.getOnlyElement(getHeaders(resp, headerName)).getValue();
  }

  static HTTPRequest copyRequest(HTTPRequest in) {
    HTTPRequest out = new HTTPRequest(in.getURL(), in.getMethod(), in.getFetchOptions());
    for (HTTPHeader h : in.getHeaders()) {
      out.addHeader(h);
    }
    out.setPayload(in.getPayload());
    return out;
  }
}
