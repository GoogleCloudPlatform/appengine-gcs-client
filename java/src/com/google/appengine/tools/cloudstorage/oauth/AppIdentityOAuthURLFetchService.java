package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityServiceFactory;
import com.google.appengine.api.utils.SystemProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

/**
 * {@link OAuthURLFetchService} based on {@link AppIdentityService}. This will work in production
 * without any need for credential passing (assuming the service account is given permission to the
 * Cloud Storage bucket), but it won't work locally running against real Google Cloud Storage. For
 * running locally, we should probably have a fake Google Storage (implementing RawGcsService backed
 * by files shouldn't be hard). For now, {@link FixedTokenOAuthURLFetchService} is good enough for
 * my experiments.
 */
final class AppIdentityOAuthURLFetchService extends AbstractOAuthURLFetchService {

  private final List<String> oauthScopes;

  AppIdentityOAuthURLFetchService(List<String> oauthScopes) {
    this.oauthScopes = ImmutableList.copyOf(oauthScopes);
  }

  @Override
  protected String getAuthorization() {
    String accessToken = AppIdentityServiceFactory.getAppIdentityService()
        .getAccessToken(oauthScopes).getAccessToken();
    if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Development) {
      throw new RuntimeException(
          this + ": The access token from the development environment won't work: " + accessToken);
    }
    return "Bearer " + accessToken;
  }

}
