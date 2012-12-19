package com.google.appengine.tools.cloudstorage;

import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.tools.cloudstorage.dev.LocalRawGcsServiceFactory;
import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory;

/**
 * Provides implementations of {@link GcsService}.
 */
public final class GcsServiceFactory {

  private static final RetryParams DEFAULT_RETRY_PARAMS = createDefaultRetryParams();

  private GcsServiceFactory() {}

  private static RetryParams createDefaultRetryParams() {
    RetryParams result = new RetryParams();
    result.setInitialRetryDelayMillis(100);
    result.setRetryMaxAttempts(6);
    result.setRetryMinAttempts(3);
    result.setRetryPeriodMillis(10000);
    return result;
  }
  
  public static GcsService createGcsService(RetryParams params) {
    RawGcsService rawGcsService;
    if (SystemProperty.environment.value() == SystemProperty.Environment.Value.Production) {
      rawGcsService = OauthRawGcsServiceFactory.createOauthRawGcsService();
    } else {
      rawGcsService = LocalRawGcsServiceFactory.createLocalRawGcsService();
    }
    return new GcsServiceImpl(rawGcsService, params);
  }
  
  public static GcsService createGcsService() {
    return createGcsService(DEFAULT_RETRY_PARAMS);
  }
  
}
