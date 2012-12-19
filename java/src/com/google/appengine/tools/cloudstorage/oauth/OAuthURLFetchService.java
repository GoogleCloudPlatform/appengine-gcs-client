package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.urlfetch.HTTPRequest;
import com.google.appengine.api.urlfetch.HTTPResponse;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * Like {@link com.google.appengine.api.urlfetch.URLFetchService}, but adds
 * OAuth headers.  Implementations define how credentials are obtained.
 *
 * <p>It is unspecified what happens if OAuth headers are already present.
 */
interface OAuthURLFetchService extends Serializable {

  HTTPResponse fetch(HTTPRequest req) throws IOException;
  Future<HTTPResponse> fetchAsync(HTTPRequest request);

}
