package com.google.appengine.tools.cloudstorage.oauth;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPMethod;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory.URLConnectionAdapter;
import com.google.appengine.tools.development.testing.LocalBlobstoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalDatastoreServiceTestConfig;
import com.google.appengine.tools.development.testing.LocalServiceTestHelper;
import com.google.appengine.tools.development.testing.LocalTaskQueueTestConfig;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

@RunWith(Parameterized.class)
public class OauthRawGcsServiceFactoryTest {

  private final static byte[] RESPONSE_1 = {'h', 'e', 'l', 'l', 'o'};
  private final static byte[] RESPONSE_2 = {'w', 'o', 'r', 'l', 'd'};
  private final static byte[] RESPONSE_3 = {'n', 'o', 't', ' ', 'f', 'o', 'u', 'n', 'd'};
  private final LocalServiceTestHelper helper = new LocalServiceTestHelper(
      new LocalTaskQueueTestConfig(), new LocalBlobstoreServiceTestConfig(),
      new LocalDatastoreServiceTestConfig());
  private HttpServer server;
  private final boolean vmEngine;
  private URL url;

  public OauthRawGcsServiceFactoryTest(boolean vmEngine) {
    this.vmEngine = vmEngine;
  }

  @Parameters
  public static Collection<Object[]> data() {
    return Arrays.asList(new Object[][]{{true}, {false}});
  }

  @Before
  public void setUp() throws Exception {
    // This is a hack to allow changing the environment variable (which is otherwise immutable)
    Map<String, String> env = System.getenv();
    Field mapField = env.getClass().getDeclaredField("m");
    mapField.setAccessible(true);
    @SuppressWarnings({"unchecked"})
    Map<String, String> map = (Map<String, String>) mapField.get(env);
    map.put("GAE_VM", String.valueOf(vmEngine));

    helper.setUp();
    server = HttpServer.create(new InetSocketAddress(0), 0);
    server.createContext("/", new HttpHandler() {
      @Override
      public void handle(HttpExchange exchange) throws IOException {
        byte[] response = RESPONSE_1;
        int code = 200;
        if ("HEAD".equals(exchange.getRequestMethod())) {
          code = 404;
          response = null;
        } else if ("POST".equals(exchange.getRequestMethod())) {
          ByteArrayOutputStream bytes = new ByteArrayOutputStream();
          ByteStreams.copy(exchange.getRequestBody(), bytes);
          if (Arrays.equals(RESPONSE_1, bytes.toByteArray())
            && "RV".equals(exchange.getRequestHeaders().getFirst("RH"))) {
            response = RESPONSE_2;
          } else {
            code = 500;
            response = RESPONSE_3;
          }
        }
        exchange.getResponseHeaders().add("Connection", "close");
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.getResponseHeaders().add("H1", "v1");
        exchange.getResponseHeaders().add("H2", "v2_1");
        exchange.getResponseHeaders().add("H2", "v2_2");
        if (response != null) {
          exchange.sendResponseHeaders(code, response.length);
          exchange.getResponseBody().write(response);
          exchange.getResponseBody().close();
        } else {
          exchange.sendResponseHeaders(code, 0);
        }
      }
    });
    server.start();
    url = new URL("http", "localhost", server.getAddress().getPort(), "/bla");
  }

  @After
  public void tearDown() {
    helper.tearDown();
    server.stop(1);
  }

  @Test
  public void testGetUrlFetchService() throws IOException {
    URLFetchService urlFetch = OauthRawGcsServiceFactory.getUrlFetchService();
    if (vmEngine) {
      assertSame(URLConnectionAdapter.class, urlFetch.getClass());
    } else {
      assertNotSame(URLConnectionAdapter.class, urlFetch.getClass());
    }
  }

  @Test
  public void testFetchService_fetchUrl() throws IOException {
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse(urlFetchService.fetch(url), 200, RESPONSE_1);
  }

  @Test
  public void testFetchService_fetchUrlAsync() throws Exception {
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse(urlFetchService.fetchAsync(url).get(), 200, RESPONSE_1);
  }

  @Test
  public void testFetchService_fetchHttpRequest() throws IOException {
    HTTPRequest req = new HTTPRequest(url, HTTPMethod.POST);
    req.setHeader(new HTTPHeader("RH", "RV"));
    req.setPayload(RESPONSE_1);
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse(urlFetchService.fetch(req), 200, RESPONSE_2);
  }

  @Test
  public void testFetchService_fetchHttpRequestFailure() throws IOException {
    HTTPRequest req = new HTTPRequest(url, HTTPMethod.HEAD);
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse(urlFetchService.fetch(req), 404, null);
  }

  @Test
  public void testFetchService_fetchHttpRequestAsync() throws Exception {
    HTTPRequest req = new HTTPRequest(url, HTTPMethod.POST);
    req.setHeader(new HTTPHeader("RH", "RV"));
    req.setPayload(RESPONSE_1);
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse(urlFetchService.fetchAsync(req).get(), 200, RESPONSE_2);
  }

  @Test
  public void testFetchService_fetchHttpRequestAsyncFailure() throws Exception {
    HTTPRequest req = new HTTPRequest(url, HTTPMethod.POST);
    req.setPayload(RESPONSE_1);
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse(urlFetchService.fetchAsync(req).get(), 500, RESPONSE_3);
  }

  private void verifyResponse(HTTPResponse response, int responseCode , byte[] expectedContent) {
    assertNull(response.getFinalUrl());
    assertEquals(responseCode, response.getResponseCode());
    assertArrayEquals(expectedContent, response.getContent());
    Map<String, Set<String>> expected = new HashMap<>();
    expected.put("H1", Sets.newHashSet("v1"));
    expected.put("H2", Sets.newHashSet("v2_1", "v2_2"));
    for (HTTPHeader h : response.getHeaders()) {
      Set<String> values = expected.get(h.getName());
      if (values != null) {
        for (String value : h.getValue().split(", ")) {
          values.remove(value);
          if (values.isEmpty()) {
            expected.remove(h.getName());
          }
        }
      }
    }
    assertTrue(expected.toString(), expected.isEmpty());
  }
}
