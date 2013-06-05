package com.google.appengine.tools.cloudstorage.oauth;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * {@link OAuthURLFetchService} that uses a fixed token, as a quick hack to allow the dev_appserver
 * to talk to real Google Cloud Storage.
 */
final class FixedTokenOAuthURLFetchService extends AbstractOAuthURLFetchService {

  private final String token;

  public FixedTokenOAuthURLFetchService(String token) {
    this.token = checkNotNull(token, "Null token");
  }

  @Override
  protected String getAuthorization() {
    return "Bearer " + token;
  }

}
