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

package com.google.appengine.tools.cloudstorage.oauth;

import static com.google.appengine.api.urlfetch.URLFetchServiceFactory.getURLFetchService;

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.tools.cloudstorage.RawGcsService;
import com.google.common.collect.ImmutableSet;


/**
 * Factory for RawGcsService using OAuth for authorization.
 */
public final class OauthRawGcsServiceFactory {

  private static final AppIdentityOAuthURLFetchService appIdFetchService =
      new AppIdentityOAuthURLFetchService(getURLFetchService(), OauthRawGcsService.OAUTH_SCOPES);

  private OauthRawGcsServiceFactory() {}

  /**
   * @param headers
   * @return a new RawGcsService
   */
  public static RawGcsService createOauthRawGcsService(ImmutableSet<HTTPHeader> headers) {
    return new OauthRawGcsService(appIdFetchService, headers);
  }
}
