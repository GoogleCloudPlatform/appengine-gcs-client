/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.cloudstorage;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.api.utils.SystemProperty;
import com.google.appengine.api.utils.SystemProperty.Environment.Value;
import com.google.appengine.tools.cloudstorage.dev.LocalRawGcsServiceFactory;
import com.google.appengine.tools.cloudstorage.oauth.AccessTokenProvider;
import com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsServiceFactory;
import com.google.apphosting.api.ApiProxy;
import com.google.apphosting.api.ApiProxy.Delegate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;

import java.util.Map;

/**
 * Provides implementations of {@link GcsService}.
 */
public final class GcsServiceFactory {

  private GcsServiceFactory() {}

  public static GcsService createGcsService(RetryParams params) {
    return createGcsService(new GcsServiceOptions.Builder().setRetryParams(params).build());
  }

  public static GcsService createGcsService(GcsServiceOptions options) {
    RawGcsService rawGcsService = createRawGcsService(options.getHttpHeaders());
    return new GcsServiceImpl(rawGcsService, options);
  }

  static RawGcsService createRawGcsService(Map<String, String> headers) {
    ImmutableSet.Builder<HTTPHeader> builder = ImmutableSet.builder();
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        builder.add(new HTTPHeader(header.getKey(), header.getValue()));
      }
    }

    RawGcsService rawGcsService;
    Value location = SystemProperty.environment.value();
    if (location == SystemProperty.Environment.Value.Production || hasCustomAccessTokenProvider()) {
      rawGcsService = OauthRawGcsServiceFactory.createOauthRawGcsService(builder.build());
    } else if (location == SystemProperty.Environment.Value.Development) {
      rawGcsService = LocalRawGcsServiceFactory.createLocalRawGcsService();
    } else {
      Delegate<?> delegate = ApiProxy.getDelegate();
      if (delegate == null
          || delegate.getClass().getName().startsWith("com.google.appengine.tools.development")) {
        rawGcsService = LocalRawGcsServiceFactory.createLocalRawGcsService();
      } else {
        rawGcsService = OauthRawGcsServiceFactory.createOauthRawGcsService(builder.build());
      }
    }
    return rawGcsService;
  }

  public static GcsService createGcsService() {
    return createGcsService(RetryParams.getDefaultInstance());
  }

  private static boolean hasCustomAccessTokenProvider() {
    return !Strings.isNullOrEmpty(System.getProperty(AccessTokenProvider.SYSTEM_PROPERTY_NAME));
  }
}
