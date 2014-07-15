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

import static com.google.appengine.tools.cloudstorage.oauth.OauthRawGcsService.makeUrl;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;

import com.google.appengine.tools.cloudstorage.GcsFilename;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Verify behaviors of OAuthRawGcsService.
 */
@RunWith(JUnit4.class)
public class OauthRawGcsServiceTest {

  private static final String BUCKET = "bucket";
  private static final String URL_PREFIX = "https://storage.googleapis.com/" + BUCKET + "/";

  @Test
  public void makeUrlShouldCorrectlyGenerateUrlWithoutUploadId() {
    String url = makeUrl(new GcsFilename(BUCKET, "object"), null).toString();
    assertEquals(URL_PREFIX + "object", url);
  }

  @Test
  public void makeUrlShouldCorrectlyGenerateUrlWithUploadId() {
    Map<String, String> queryStrings = singletonMap("upload_id", "1");
    String url = makeUrl(new GcsFilename(BUCKET, "object"), queryStrings).toString();
    assertEquals(URL_PREFIX + "object?upload_id=1", url);
  }

  @Test
  public void makeUrlShouldCorrectlyEncodeObjectName() {
    Map<String, String> queryStrings = emptyMap();
    String url = makeUrl(new GcsFilename(BUCKET, "~obj%ect sp{ace>"), queryStrings).toString();
    assertEquals(URL_PREFIX + "~obj%25ect%20sp%7Bace%3E", url);
  }

  @Test
  public void makeUrlShouldCorrectlyEncodeQueryString() {
    Map<String, String> queryStrings = new LinkedHashMap<>();
    queryStrings.put("upload_id", "} 20");
    queryStrings.put("composed", null);
    queryStrings.put("val=ue", "%7D&");
    queryStrings.put("regular", "val");
    queryStrings.put("k e+y", "=v a+lu&e=");
    String url = makeUrl(new GcsFilename(BUCKET, "object"), queryStrings).toString();
    String expected = URL_PREFIX + "object?"
        + "upload_id=%7D+20&"
        + "composed&"
        + "val%3Due=%257D%26&"
        + "regular=val&"
        + "k+e%2By=%3Dv+a%2Blu%26e%3D";
    assertEquals(expected, url);
  }
}
