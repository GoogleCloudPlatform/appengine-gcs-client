package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.appengine.api.urlfetch.URLFetchServiceFactory;

import java.io.IOException;
import java.util.concurrent.Future;

/**
 * An OAuthURLFetchService decorator that adds the authorization header to the http request.
 */
abstract class AbstractOAuthURLFetchService implements OAuthURLFetchService {
  private static final long serialVersionUID = 839709682488277548L;

  private static final URLFetchService URLFETCH = URLFetchServiceFactory.getURLFetchService();

  AbstractOAuthURLFetchService() {}

  protected abstract String getAuthorization();

  private HTTPRequest authorizeRequest(HTTPRequest req) {
    req = URLFetchUtils.copyRequest(req);
    req.setHeader(new HTTPHeader("Authorization", getAuthorization()));
    return req;
  }

  @Override
  public HTTPResponse fetch(HTTPRequest req) throws IOException {
    return URLFETCH.fetch(authorizeRequest(req));
  }

  @Override
  public Future<HTTPResponse> fetchAsync(HTTPRequest req) {
    return URLFETCH.fetchAsync(authorizeRequest(req));
  }


}
