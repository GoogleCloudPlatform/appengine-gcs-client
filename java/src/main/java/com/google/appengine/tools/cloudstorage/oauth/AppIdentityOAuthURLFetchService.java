/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.appengine.tools.cloudstorage.oauth;

import com.google.appengine.api.appidentity.AppIdentityService;
import com.google.appengine.api.appidentity.AppIdentityService.GetAccessTokenResult;
import com.google.appengine.api.urlfetch.URLFetchService;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

/**
 * {@link OAuthURLFetchService} based on {@link AppIdentityService}. This will work in production
 * without any need for credential passing (assuming the service account is given permission to the
 * Google Cloud Storage bucket), but it won't work locally running against real Google Cloud
 * Storage.
 */
final class AppIdentityOAuthURLFetchService extends AbstractOAuthURLFetchService {
  private static final Logger logger =
      Logger.getLogger(AppIdentityOAuthURLFetchService.class.getCanonicalName());

  /**
   * A range of time is provided for the refresh so that multiple instance don't all attempt to
   * refresh at the same time.
   */
  private static final int MAX_CACHE_EXPIRATION_HEADROOM = 300000;
  private static final int MIN_CACHE_EXPIRATION_HEADROOM = 60000;

  private final Random rand = new Random();
  private final Lock lock = new ReentrantLock();
  private final AccessTokenProvider accessTokenProvider;
  private final List<String> oauthScopes;
  private final AtomicReference<GetAccessTokenResult> accessToken = new AtomicReference<>();
  private final AtomicInteger cacheExpirationHeadroom =
      new AtomicInteger(getNextCacheExpirationHeadroom());

  /**
   * Used to prevent multiple requests from being issued in parallel from the same instance.
   */
  private final AtomicBoolean refreshInProgress = new AtomicBoolean(false);

  AppIdentityOAuthURLFetchService(URLFetchService urlFetch, List<String> oauthScopes) {
    super(urlFetch);
    this.oauthScopes = ImmutableList.copyOf(oauthScopes);
    this.accessTokenProvider = createAccessTokenProvider();
  }

  /**
   * Attempts to return the token from cache. If this is not possible because it is expired or was
   * never assigned, a new token is requested and parallel requests will block on retrieving a new
   * token. As such no guarantee of maximum latency is provided.
   *
   * To avoid blocking the token is refreshed before it's expiration, while parallel requests
   * continue to use the old token.
   */
  @Override
  protected String getToken() {
    GetAccessTokenResult token = accessToken.get();
    if (token == null || isExpired(token)
        || (isAboutToExpire(token) && refreshInProgress.compareAndSet(false, true))) {
      lock.lock();
      try {
        token = accessToken.get();
        if (token == null || isAboutToExpire(token)) {
          token = accessTokenProvider.getNewAccessToken(oauthScopes);
          accessToken.set(token);
          cacheExpirationHeadroom.set(getNextCacheExpirationHeadroom());
        }
      } finally {
        refreshInProgress.set(false);
        lock.unlock();
      }
    }
    return token.getAccessToken();
  }

  private AccessTokenProvider createAccessTokenProvider() {
    String providerClassName =
        Strings.emptyToNull(System.getProperty(AccessTokenProvider.SYSTEM_PROPERTY_NAME));

    if (providerClassName != null) {
      try {
        @SuppressWarnings("unchecked")
        Class<AccessTokenProvider> providerClass = (Class<AccessTokenProvider>)
            Class.forName(providerClassName);

        return providerClass.newInstance();
      } catch (ClassCastException | ClassNotFoundException | InstantiationException
          | IllegalAccessException e) {
        logger.warning(String.format(
            "Unable to create AccessTokenProvider '%s'; falling back to default. Exception: %s\b",
            providerClassName, e));
      }
    }

    return new AppIdentityAccessTokenProvider();
  }

  private boolean isExpired(GetAccessTokenResult token) {
    return token.getExpirationTime().getTime() < System.currentTimeMillis();
  }

  private boolean isAboutToExpire(GetAccessTokenResult token) {
    long now = System.currentTimeMillis();
    return token.getExpirationTime().getTime() - cacheExpirationHeadroom.get() < now;
  }

  private final int getNextCacheExpirationHeadroom() {
    return rand.nextInt(MAX_CACHE_EXPIRATION_HEADROOM - MIN_CACHE_EXPIRATION_HEADROOM)
        + MIN_CACHE_EXPIRATION_HEADROOM;
  }
}
