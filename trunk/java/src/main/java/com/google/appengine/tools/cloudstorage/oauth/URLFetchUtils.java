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
import java.util.Date;

/**
 * URLFetch-related utilities.
 */
final class URLFetchUtils {

  @VisibleForTesting
  static final DateTimeFormatter DATE_FORMAT =
      DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss 'GMT'").withZoneUTC();

  private URLFetchUtils() {}

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

  private static void appendRequest(HttpRequest req, StringBuilder b) {
    b.append(req.getRequestMethod()).append(' ').append(req.getUrl());
    HttpHeaders headers = req.getHeaders();
    for (String name : req.getHeaders().keySet()) {
      b.append('\n').append(name).append(": ").append(headers.get(name));
    }
    b.append("\n\n");
    if (req.getContent() == null) {
      b.append("no content");
    } else {
      try {
        b.append(req.getContent().getLength()).append(" bytes of content");
      } catch (IOException e) {
        b.append("could not read content length");
      }
    }
    b.append('\n');
  }

  static void appendRequest(HTTPRequest req, StringBuilder b) {
    b.append(req.getMethod()).append(' ').append(req.getURL());
    for (HTTPHeader h : req.getHeaders()) {
      b.append('\n').append(h.getName()).append(": ").append(h.getValue());
    }
    b.append("\n\n");
    if (req.getPayload() == null) {
      b.append("no content");
    } else {
      b.append(req.getPayload().length).append(" bytes of content");
    }
    b.append('\n');
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

  private static void appendResponse(HTTPResponse resp, StringBuilder b) {
    byte[] content = resp.getContent();
    b.append(resp.getResponseCode()).append(" with ").append(content == null ? 0 : content.length);
    b.append(" bytes of content");
    for (HTTPHeader h : resp.getHeadersUncombined()) {
      b.append('\n').append(h.getName()).append(": ").append(h.getValue());
    }
    b.append('\n').append(content == null ? "" : new String(content, UTF_8)).append('\n');
  }

  static String describeRequestAndResponse(HttpRequest req, HttpResponseException resp) {
    StringBuilder stBuilder = new StringBuilder(256).append("Request: ");
    appendRequest(req, stBuilder);
    stBuilder.append("\nResponse: ");
    appendResponse(resp, stBuilder);
    return stBuilder.toString();
  }

  static String describeRequestAndResponse(HTTPRequest req, HTTPResponse resp) {
    StringBuilder stBuilder = new StringBuilder(256).append("Request: ");
    appendRequest(req, stBuilder);
    stBuilder.append("\nResponse: ");
    appendResponse(resp, stBuilder);
    return stBuilder.toString();
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
