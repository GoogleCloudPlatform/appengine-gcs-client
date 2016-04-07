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

import static com.google.appengine.api.urlfetch.HTTPMethod.PUT;
import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.makeUrl;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import com.google.appengine.api.urlfetch.FetchOptions;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.tools.cloudstorage.GcsFilename;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Verify behaviors of {@link OauthRawGcsService}. */
@RunWith(JUnit4.class)
public class OauthRawGcsServiceTest {

  private static final String BUCKET = "bucket";
  private static final FetchOptions FETCH_OPTIONS = FetchOptions.Builder
        .disallowTruncate()
        .doNotFollowRedirects()
        .validateCertificate()
        .setDeadline(30.0);
  private static final GcsFilename GCS_FILENAME = new GcsFilename("sample-bucket", "sample-object");
  private static final ImmutableSet<HTTPHeader> HEADERS = ImmutableSet.of();
  private static final String URL_PREFIX = "https://storage.googleapis.com/" + BUCKET + "/";

  private OAuthURLFetchService oauthURLFetchService;
  private OauthRawGcsService service;

  @Before
  public void setUp() {
    oauthURLFetchService = mock(OAuthURLFetchService.class);
    service = new OauthRawGcsService(oauthURLFetchService, HEADERS);
  }

  @Test
  public void makeRequestShouldHandleAnAbsentPayload() throws MalformedURLException {
    URL url = new URL("https://storage.googleapis.com/sample-bucket/sample-object");
    HTTPRequest expected = new HTTPRequest(url, PUT, FETCH_OPTIONS);
    expected.addHeader(new HTTPHeader("Content-Length", "0"));
    expected.addHeader(new HTTPHeader("User-Agent", OauthRawGcsService.USER_AGENT_PRODUCT));
    assertHttpRequestEquals(expected, service.makeRequest(GCS_FILENAME, null, PUT, 30000));
  }

  @Test
  public void makeRequestShouldHandleAnEmptyPayload() throws MalformedURLException {
    URL url = new URL("https://storage.googleapis.com/sample-bucket/sample-object");
    HTTPRequest expected = new HTTPRequest(url, PUT, FETCH_OPTIONS);
    expected.addHeader(new HTTPHeader("Content-Length", "0"));
    expected.addHeader(new HTTPHeader("User-Agent", OauthRawGcsService.USER_AGENT_PRODUCT));
    assertHttpRequestEquals(
        expected, service.makeRequest(GCS_FILENAME, null, PUT, 30000, new byte[0]));
  }

  @Test
  public void makeRequestShouldHandleAPresentPayload() throws MalformedURLException {
    URL url = new URL("https://storage.googleapis.com/sample-bucket/sample-object");
    byte[] payload = "hello".getBytes(UTF_8);
    HTTPRequest expected = new HTTPRequest(url, PUT, FETCH_OPTIONS);
    expected.addHeader(new HTTPHeader("Content-Length", "5"));
    expected.addHeader(new HTTPHeader("Content-MD5", "XUFAKrxLKna5cZ2REBfFkg=="));
    expected.addHeader(new HTTPHeader("User-Agent", OauthRawGcsService.USER_AGENT_PRODUCT));
    expected.setPayload(payload);
    assertHttpRequestEquals(expected, service.makeRequest(GCS_FILENAME, null, PUT, 30000, payload));
  }

  @Test
  public void makeUrlShouldCorrectlyGenerateUrlWithoutUploadId() {
    String url = makeUrl(new GcsFilename(BUCKET, "object"), null).toString();
    assertEquals(URL_PREFIX + "object", url);
  }

  @Test
  public void makeUrlShouldCorrectlyGenerateUrlWithUploadId() {
    Map<String, String> queryStrings = singletonMap("upload_id", "1");
    String url = makeUrl(new GcsFilename(BUCKET, "object"), queryStrings).toString();
    assertEquals(URL_PREFIX + "object?upload_id=1", url);
  }

  @Test
  public void makeUrlShouldCorrectlyEncodeObjectName() {
    Map<String, String> queryStrings = emptyMap();
    String url = makeUrl(new GcsFilename(BUCKET, "~obj%ect sp{ace>"), queryStrings).toString();
    assertEquals(URL_PREFIX + "~obj%25ect%20sp%7Bace%3E", url);
  }

  @Test
  public void makeUrlShouldCorrectlyEncodeQueryString() {
    Map<String, String> queryStrings = new LinkedHashMap<>();
    queryStrings.put("upload_id", "} 20");
    queryStrings.put("composed", null);
    queryStrings.put("val=ue", "%7D&");
    queryStrings.put("regular", "val");
    queryStrings.put("k e+y", "=v a+lu&e=");
    String url = makeUrl(new GcsFilename(BUCKET, "object"), queryStrings).toString();
    String expected = URL_PREFIX + "object?"
        + "upload_id=%7D+20&"
        + "composed&"
        + "val%3Due=%257D%26&"
        + "regular=val&"
        + "k+e%2By=%3Dv+a%2Blu%26e%3D";
    assertEquals(expected, url);
  }

  private void assertHttpRequestEquals(HTTPRequest req1, HTTPRequest req2) {
    Map<String, String> req1Headers = toMap(req1.getHeaders());
    Map<String, String> req2Headers = toMap(req2.getHeaders());

    assertArrayEquals(req1.getPayload(), req2.getPayload());
    assertEquals(req1.getURL().toString(), req2.getURL().toString());
    assertEquals(req1.getMethod(), req2.getMethod());
    assertEquals(req1Headers.size(), req2Headers.size());
    for (Map.Entry<String, String> entry : req1Headers.entrySet()) {
      assertEquals(entry.getValue(), req2Headers.get(entry.getKey()));
    }
  }

  private Map<String, String> toMap(List<HTTPHeader> headers) {
    return Maps.transformValues(
        Maps.uniqueIndex(headers, GET_HEADER_NAME),
        GET_HEADER_VALUE);
  }

  private static final Function<HTTPHeader, String> GET_HEADER_NAME =
      new Function<HTTPHeader, String>() {
    @Override public String apply(HTTPHeader header) {
      return header.getName();
    }
  };

  private static final Function<HTTPHeader, String> GET_HEADER_VALUE =
      new Function<HTTPHeader, String>() {
    @Override public String apply(HTTPHeader header) {
      return header.getValue();
    }
  };
}
