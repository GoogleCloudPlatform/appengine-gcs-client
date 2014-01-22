package com.google.appengine.tools.cloudstorage.oauth;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.tools.cloudstorage.RetryHelperException;
import com.google.apphosting.api.ApiProxy.ApiDeadlineExceededException;
import com.google.common.collect.Lists;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Tests error handling around fetching credentials.
 */
@RunWith(JUnit4.class)
public class AppIdenityOauthTest {

  List<String> oauthScopes = Lists.newArrayList("foo", "bar");

  private static class FailingFetchService extends AbstractOAuthURLFetchService {

    private int numToFail;

    FailingFetchService(URLFetchService urlFetch, int numToFail) {
      super(urlFetch);
      this.numToFail = numToFail;
    }

    @Override
    protected String getToken() {
      if (numToFail <= 0) {
        return "";
      } else {
        numToFail--;
        throw new ApiDeadlineExceededException(null, null);
      }
    }
  }

  @Test
  public void testAuthIsRetried() throws IOException, InterruptedException, ExecutionException {
    URLFetchService urlFetchService = mock(URLFetchService.class, RETURNS_MOCKS);
    FailingFetchService failingFetchService = new FailingFetchService(urlFetchService, 1);
    failingFetchService.fetch(mock(HTTPRequest.class));

    failingFetchService = new FailingFetchService(urlFetchService, 1);
    failingFetchService.fetchAsync(mock(HTTPRequest.class)).get();
  }

  @Test
  public void testErrorsArePropigated() throws IOException, InterruptedException {
    URLFetchService urlFetchService = mock(URLFetchService.class, RETURNS_MOCKS);
    FailingFetchService failingFetchService =
        new FailingFetchService(urlFetchService, Integer.MAX_VALUE);
    try {
      failingFetchService.fetch(mock(HTTPRequest.class));
      fail();
    } catch (RetryHelperException e) {
    }
    Future<HTTPResponse> fetchAsync = failingFetchService.fetchAsync(mock(HTTPRequest.class));
    try {
      fetchAsync.get();
      fail();
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof RetryHelperException);
    }
  }

}
