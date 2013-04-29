package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.tools.cloudstorage.RawGcsService;


public final class OauthRawGcsServiceFactory {

  private OauthRawGcsServiceFactory() {}

  /**
   * @return a new RawGcsService
   */
  public static RawGcsService createOauthRawGcsService() {
    return new OauthRawGcsService(
        new AppIdentityOAuthURLFetchService(OauthRawGcsService.OAUTH_SCOPES));
  }

}
