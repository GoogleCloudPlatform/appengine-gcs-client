package com.google.appengine.tools.cloudstorage;

import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.dev.LocalRawGcsServiceFactory;
import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory;

/**
 * Provides implementations of {@link GcsService}.
 */
public final class GcsServiceFactory {

  private GcsServiceFactory() {}

  public static GcsService createGcsService(RetryParams params) {
    RawGcsService rawGcsService = createRawGcsService();
    return new GcsServiceImpl(rawGcsService, params);
  }

  static RawGcsService createRawGcsService() {
    RawGcsService rawGcsService;
    if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) {
      rawGcsService = OauthRawGcsServiceFactory.createOauthRawGcsService();
    } else {
      rawGcsService = LocalRawGcsServiceFactory.createLocalRawGcsService();
    }
    return rawGcsService;
  }

  public static GcsService createGcsService() {
    return createGcsService(RetryParams.getDefaultInstance());
  }

}
