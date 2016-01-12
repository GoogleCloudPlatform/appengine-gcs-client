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
        if ("POST".equals(exchange.getRequestMethod())) {
          ByteArrayOutputStream bytes = new ByteArrayOutputStream();
          ByteStreams.copy(exchange.getRequestBody(), bytes);
          if (Arrays.equals(RESPONSE_1, bytes.toByteArray())
            && "RV".equals(exchange.getRequestHeaders().getFirst("RH"))) {
            response = RESPONSE_2;
          } else {
            code = 500;
          }
        }
        exchange.getResponseHeaders().add("Connection", "close");
        exchange.getResponseHeaders().add("Content-Type", "text/plain");
        exchange.getResponseHeaders().add("H1", "v1");
        exchange.getResponseHeaders().add("H2", "v2_1");
        exchange.getResponseHeaders().add("H2", "v2_2");
        exchange.sendResponseHeaders(code, response.length);
        exchange.getResponseBody().write(response);
        exchange.getResponseBody().close();
      }
    });
    server.start();
    url = new URL("http", server.getAddress().getHostName(), server.getAddress().getPort(), "/bla");
  }

  @After
  public void tearDown() {
    helper.tearDown();
    server.stop(1);
  }

  @Test
  public void testGetUrlFetchService() throws Exception {
    URLFetchService urlFetch = OauthRawGcsServiceFactory.getUrlFetchService();
    if (vmEngine) {
      assertSame(OauthRawGcsServiceFactory.URLConnectionAdapter.class, urlFetch.getClass());
    } else {
      assertNotSame(OauthRawGcsServiceFactory.URLConnectionAdapter.class, urlFetch.getClass());
    }
  }

  @Test
  public void testFetchService_fetchUrl() throws Exception {
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse1(urlFetchService.fetch(url));
  }

  @Test
  public void testFetchService_fetchUrlAsync() throws Exception {
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse1(urlFetchService.fetchAsync(url).get());
  }

  @Test
  public void testFetchService_fetchHttpRequest() throws Exception {
    HTTPRequest req = new HTTPRequest(url, HTTPMethod.POST);
    req.setHeader(new HTTPHeader("RH", "RV"));
    req.setPayload(RESPONSE_1);
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse2(urlFetchService.fetch(req));
  }

  @Test
  public void testFetchService_fetchHttpRequestAsync() throws Exception {
    HTTPRequest req = new HTTPRequest(url, HTTPMethod.POST);
    req.setHeader(new HTTPHeader("RH", "RV"));
    req.setPayload(RESPONSE_1);
    URLFetchService urlFetchService = OauthRawGcsServiceFactory.getUrlFetchService();
    verifyResponse2(urlFetchService.fetchAsync(req).get());
  }

  private void verifyResponse1(HTTPResponse response) throws IOException {
    verifyResponse(response, RESPONSE_1);
  }

  private void verifyResponse2(HTTPResponse response) throws IOException {
    verifyResponse(response, RESPONSE_2);
  }

  private void verifyResponse(HTTPResponse response, byte[] expectedContent) {
    assertNull(response.getFinalUrl());
    assertEquals(200, response.getResponseCode());
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
