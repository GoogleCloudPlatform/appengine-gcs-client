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

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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

  private static void appendResponse(HTTPResponse resp, boolean includeBody, StringBuilder b) {
    b.append(resp.getResponseCode()).append(" with ").append(resp.getContent().length);
    b.append(" bytes of content");
    for (HTTPHeader h : resp.getHeadersUncombined()) {
      b.append('\n').append(h.getName()).append(": ").append(h.getValue());
    }
    if (includeBody) {
      b.append('\n').append(new String(resp.getContent(), UTF_8));
    } else {
      b.append("\n<content elided>");
    }
    b.append('\n');
  }

  static String describeRequestAndResponse(HTTPRequest req, HTTPResponse resp,
      boolean includeResponseBody) {
    StringBuilder stBuilder = new StringBuilder(256).append("Request: ");
    appendRequest(req, stBuilder);
    stBuilder.append("\nResponse: ");
    appendResponse(resp, includeResponseBody, stBuilder);
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
