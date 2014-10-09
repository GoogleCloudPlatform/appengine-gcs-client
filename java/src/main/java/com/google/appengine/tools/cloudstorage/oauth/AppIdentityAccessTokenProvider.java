package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.utils.SystemProperty;

import java.util.List;

/**
 * Provider that uses the AppIdentityService for generating access tokens.
 */
final class AppIdentityAccessTokenProvider implements AccessTokenProvider {
  private final AppIdentityService appIdentityService;

  public AppIdentityAccessTokenProvider() {
    this.appIdentityService = AppIdentityServiceFactory.getAppIdentityService();
  }

  @Override
  public GetAccessTokenResult getNewAccessToken(List<String> scopes) {
    if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Development) {
      throw new IllegalStateException(
          "The access token from AppIdentity won't work in the development environment.");
    }
    return appIdentityService.getAccessToken(scopes);
  }
}
