package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult;

import java.util.List;

/**
 * Interface for users to implement for providing an access token to be used when authenticating
 * with Google Cloud storage.
 */
public interface AccessTokenProvider {
  public static final String SYSTEM_PROPERTY_NAME = "gcs_access_token_provider";

  /**
   * Creates a fresh access token. This may make a round trip call to the server.
   */
  GetAccessTokenResult getNewAccessToken(List<String> scopes);
}
