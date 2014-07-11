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
import static org.junit.Assert.assertEquals;

import com.google.appengine.tools.cloudstorage.GcsFilename;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 * Verify behaviors of OAuthRawGcsService.
 */
@RunWith(JUnit4.class)
public class OauthRawGcsServiceTest {

  @Test
  public void makeUrlShouldCorrectlyGenerateUrlWithoutUploadId() {
    String url = makeUrl(new GcsFilename("bucket", "object"), null).toString();
    assertEquals("https://storage.googleapis.com/bucket/object", url);
  }

  @Test
  public void makeUrlShouldCorrectlyGenerateUrlWithUploadId() {
    String url = makeUrl(new GcsFilename("bucket", "object"), "upload_id=1").toString();
    assertEquals("https://storage.googleapis.com/bucket/object?upload_id=1", url);
  }

  @Test
  public void makeUrlShouldCorrectlyEncodeObjectName() {
    String url = makeUrl(new GcsFilename("bucket", "~obj%ect sp{ace>"), null).toString();
    assertEquals("https://storage.googleapis.com/bucket/~obj%25ect%20sp%7Bace%3E", url);
  }

  @Test
  public void makeUrlShouldCorrectlyEncodeUploadId() {
    String url = makeUrl(new GcsFilename("bucket", "object"), "upload_id=} 20").toString();
    assertEquals("https://storage.googleapis.com/bucket/object?upload_id=%7D%2020", url);
  }
}
