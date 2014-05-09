/*
 * Copyright 2014 Google Inc. All Rights Reserved.
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

import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_INITIAL_RETRY_DELAY_MILLIS;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_MAX_REQUEST_TIMEOUT;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_MAX_RETRY_DELAY_MILLIS;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_REQUEST_TIMEOUT_MILLIS;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_REQUEST_TIMEOUT_RETRY_FACTOR;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_RETRY_DELAY_BACKOFF_FACTOR;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_RETRY_MAX_ATTEMPTS;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_RETRY_MIN_ATTEMPTS;
import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_TOTAL_RETRY_PERIOD_MILLIS;
import static java.lang.Math.pow;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.stub;

import com.google.appengine.tools.cloudstorage.RetryHelper.Context;
import com.google.appengine.tools.cloudstorage.RetryParams.Builder;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.Arrays;

/**
 * Tests for {@link RetryParams}.
 *
 */
@RunWith(JUnit4.class)
public class RetryParamsTest {

  @Test
  public void testDefaults() {
    RetryParams params1 = RetryParams.getDefaultInstance();
    RetryParams params2 = new RetryParams.Builder().build();
    for (RetryParams params : Arrays.asList(params1, params2)) {
      assertEquals(DEFAULT_INITIAL_RETRY_DELAY_MILLIS, params.getInitialRetryDelayMillis());
      assertEquals(DEFAULT_MAX_REQUEST_TIMEOUT, params.getMaxRequestTimeout());
      assertEquals(DEFAULT_MAX_RETRY_DELAY_MILLIS, params.getMaxRetryDelayMillis());
      assertEquals(DEFAULT_REQUEST_TIMEOUT_MILLIS, params.getRequestTimeoutMillis());
      assertEquals(DEFAULT_REQUEST_TIMEOUT_MILLIS,
          params.getRequestTimeoutMillisForCurrentAttempt());
      assertEquals(DEFAULT_REQUEST_TIMEOUT_RETRY_FACTOR, params.getRequestTimeoutRetryFactor(), 0);
      assertEquals(DEFAULT_RETRY_DELAY_BACKOFF_FACTOR, params.getRetryDelayBackoffFactor(), 0);
      assertEquals(DEFAULT_RETRY_MAX_ATTEMPTS, params.getRetryMaxAttempts());
      assertEquals(DEFAULT_RETRY_MIN_ATTEMPTS, params.getRetryMinAttempts());
      assertEquals(DEFAULT_TOTAL_RETRY_PERIOD_MILLIS, params.getTotalRetryPeriodMillis());
    }
  }

  @Test
  public void testSetAndCopy() {
    RetryParams.Builder builder = new RetryParams.Builder();
    builder.initialRetryDelayMillis(101);
    builder.maxRetryDelayMillis(102);
    builder.retryDelayBackoffFactor(103);
    builder.requestTimeoutMillis(104);
    builder.requestTimeoutRetryFactor(105);
    builder.maxRequestTimeout(106);
    builder.retryMinAttempts(107);
    builder.retryMaxAttempts(108);
    builder.totalRetryPeriodMillis(109);
    RetryParams params1 = builder.build();
    RetryParams params2 = new RetryParams.Builder(params1).build();
    for (RetryParams params : Arrays.asList(params1, params2)) {
      assertEquals(101, params.getInitialRetryDelayMillis());
      assertEquals(102, params.getMaxRetryDelayMillis());
      assertEquals(103, params.getRetryDelayBackoffFactor(), 0);
      assertEquals(104, params.getRequestTimeoutMillis());
      assertEquals(104, params.getRequestTimeoutMillisForCurrentAttempt());
      assertEquals(105, params.getRequestTimeoutRetryFactor(), 0);
      assertEquals(106, params.getMaxRequestTimeout());
      assertEquals(107, params.getRetryMinAttempts());
      assertEquals(108, params.getRetryMaxAttempts());
      assertEquals(109, params.getTotalRetryPeriodMillis());
    }
  }

  @Test
  public void testBadSettings() {
    RetryParams.Builder builder = new RetryParams.Builder();
    builder.initialRetryDelayMillis(-1);
    builder = verifyFailure(builder);
    builder.maxRetryDelayMillis(RetryParams.getDefaultInstance().getInitialRetryDelayMillis() - 1);
    builder = verifyFailure(builder);
    builder.retryDelayBackoffFactor(-1);
    builder = verifyFailure(builder);
    builder.requestTimeoutMillis(-1);
    builder = verifyFailure(builder);
    builder.requestTimeoutRetryFactor(-1);
    builder = verifyFailure(builder);
    builder.maxRequestTimeout(RetryParams.getDefaultInstance().getRequestTimeoutMillis() - 1);
    builder = verifyFailure(builder);
    builder.retryMinAttempts(-1);
    builder = verifyFailure(builder);
    builder.retryMaxAttempts(RetryParams.getDefaultInstance().getRetryMinAttempts() - 1);
    builder = verifyFailure(builder);
    builder.totalRetryPeriodMillis(-1);
    builder = verifyFailure(builder);
    builder.retryMaxAttempts(RetryParams.getDefaultInstance().getRetryMinAttempts());
    builder.maxRetryDelayMillis(RetryParams.getDefaultInstance().getInitialRetryDelayMillis());
    builder.maxRequestTimeout(RetryParams.getDefaultInstance().getRequestTimeoutMillis());
    builder.build();
  }

  private static Builder verifyFailure(Builder builder) {
    try {
      builder.build();
      fail("Expected IllegalArgumentException");
    } catch (IllegalArgumentException ex){
    }
    return new RetryParams.Builder();
  }

  private static void verifyRequestTimeoutMillisForCurrentAttempt(RetryParams params,
      int value, double timeout) {
    Context ctx = mock(Context.class);
    stub(ctx.getAttemptNumber()).toReturn(value);
    RetryHelper.setContext(ctx);
    assertEquals(timeout, params.getRequestTimeoutMillisForCurrentAttempt(), 1);
    RetryHelper.setContext(null);
  }

  @Test
  public void testRequestTimeoutAttempts() {
    RetryParams params = new RetryParams.Builder().requestTimeoutRetryFactor(1.2).build();
    long timeout = params.getRequestTimeoutMillis();
    double factor = params.getRequestTimeoutRetryFactor();
    long max = params.getMaxRequestTimeout();
    assertEquals(timeout, params.getRequestTimeoutMillisForCurrentAttempt());
    verifyRequestTimeoutMillisForCurrentAttempt(params, 0, timeout);
    verifyRequestTimeoutMillisForCurrentAttempt(params, 1, timeout);
    for (int i = 2; i < 5; i++) {
      verifyRequestTimeoutMillisForCurrentAttempt(params, i, timeout * pow(factor, i - 1));
    }
    for (int i = 5; i < 100; i += 10) {
      verifyRequestTimeoutMillisForCurrentAttempt(params, i, max);
    }
    verifyRequestTimeoutMillisForCurrentAttempt(params, Integer.MAX_VALUE, max);
    params = new RetryParams.Builder().requestTimeoutRetryFactor(Double.MAX_VALUE).build();
    verifyRequestTimeoutMillisForCurrentAttempt(params, Integer.MAX_VALUE, max);
  }
}
