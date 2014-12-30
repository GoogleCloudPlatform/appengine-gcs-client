package com.google.appengine.tools.cloudstorage.oauth;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.api.client.http.HttpRequest;
import com.google.api.client.http.HttpRequestInitializer;
import com.google.api.client.http.HttpResponseException;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.storage.Storage;
import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.tools.cloudstorage.oauth.URLFetchUtils.HTTPRequestInfo;
import com.google.common.collect.ImmutableList;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.net.URL;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.NoSuchElementException;


/**
 * Tests {@link URLFetchUtils#parseDate(String)}
 */
@RunWith(JUnit4.class)
public class URLFetchUtilsTest {

  @Test
  public void testParseDateRoundtrip() {
    Date now = new Date((System.currentTimeMillis() / 1000) * 1000);
    String printed = URLFetchUtils.DATE_FORMAT.print(now.getTime());
    assertEquals(now.getTime(), URLFetchUtils.parseDate(printed).getTime());
  }

  @Test
  public void testParseDateSampleDate() {
    String printed = "Tue, 22 Oct 2013 19:28:45 GMT";
    Date date = URLFetchUtils.parseDate(printed);
    assertNotNull(date);

    DateTimeFormatter gcsFormat =
        DateTimeFormat.forPattern("EEE, dd MMM yyyy HH:mm:ss z").withZoneUTC()
            .withLocale(Locale.US);
    assertEquals(printed.substring(0, printed.length() - 4),
        gcsFormat.print(date.getTime()).substring(0, printed.length() - 4));
  }

  @Test
  public void testDescribeRequestAndResponseForApiClient() throws Exception {
    HttpRequestInitializer initializer = mock(HttpRequestInitializer.class);
    NetHttpTransport transport = new NetHttpTransport();
    Storage storage = new Storage.Builder(transport, new JacksonFactory(), initializer)
        .setApplicationName("bla").build();
    HttpRequest request = storage.objects().delete("b", "o").buildHttpRequest();
    request.getHeaders().clear();
    request.getHeaders().put("k1", "v1");
    request.getHeaders().put("k2", "v2");
    HttpResponseException exception = null;
    try {
      request.execute();
    } catch (HttpResponseException ex) {
      exception = ex;
    }
    String expected = "Request: DELETE " + Storage.DEFAULT_BASE_URL + "b/b/o/o\n"
        + "k1: v1\nk2: v2\n\nno content\n\nResponse: 400 with 0 bytes of content\n";
    String result =
        URLFetchUtils.describeRequestAndResponse(new HTTPRequestInfo(request), exception);
    assertTrue(expected + "\nis not a prefix of:\n" + result, result.startsWith(expected));
  }

  @Test
  public void testDescribeRequestAndResponseF() throws Exception {
    HTTPRequest request = new HTTPRequest(new URL("http://ping/pong"));
    request.setPayload("hello".getBytes());
    request.addHeader(new HTTPHeader("k1", "v1"));
    request.addHeader(new HTTPHeader("k2", "v2"));
    HTTPResponse response = mock(HTTPResponse.class);
    when(response.getHeadersUncombined()).thenReturn(ImmutableList.of(new HTTPHeader("k3", "v3")));
    when(response.getResponseCode()).thenReturn(500);
    when(response.getContent()).thenReturn("bla".getBytes());
    String expected = "Request: GET http://ping/pong\nk1: v1\nk2: v2\n\n"
        + "5 bytes of content\n\nResponse: 500 with 3 bytes of content\nk3: v3\nbla\n";
    String result =
        URLFetchUtils.describeRequestAndResponse(new HTTPRequestInfo(request), response);
    assertEquals(expected, result);
  }

  @Test
  public void testGetSingleHeader() {
    HTTPResponse response = mock(HTTPResponse.class);
    try {
      URLFetchUtils.getSingleHeader(response, "k1");
      fail("NoSuchElementException expected");
    } catch (NoSuchElementException expected) {
    }

    List<HTTPHeader> headers =
        ImmutableList.of(new HTTPHeader("k3", "v3"), new HTTPHeader("k1", "v1"));
    when(response.getHeadersUncombined()).thenReturn(headers);
    assertEquals("v1", URLFetchUtils.getSingleHeader(response, "k1"));

    headers = ImmutableList.of(new HTTPHeader("k3", "v3"), new HTTPHeader("k1", "v1"),
        new HTTPHeader("k1", "v2"));
    when(response.getHeadersUncombined()).thenReturn(headers);
    try {
      URLFetchUtils.getSingleHeader(response, "k1");
      fail("NoSuchElementException expected");
    } catch (IllegalArgumentException  expected) {
    }
  }

  @Test
  public void testCopyRequest() throws Exception {
    HTTPRequest request = new HTTPRequest(new URL("http://h1/v1"), HTTPMethod.HEAD);
    request.setHeader(new HTTPHeader("k3", "v3"));
    HTTPRequest copy = URLFetchUtils.copyRequest(request);
    assertEquals("http://h1/v1", copy.getURL().toString());
    assertEquals(HTTPMethod.HEAD, copy.getMethod());
    assertEquals(1, copy.getHeaders().size());
    assertEquals("k3", copy.getHeaders().get(0).getName());
    assertEquals("v3", copy.getHeaders().get(0).getValue());
  }
}
