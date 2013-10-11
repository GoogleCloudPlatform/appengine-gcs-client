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

import java.io.Serializable;

/**
 * Parameters for configuring exponential backoff. See {@link RetryHelper}. The initial request is
 * executed immediately. It is given {@code requestTimeoutMillis} to complete or it is regarded as
 * a failure. If the request fails the calling thread sleeps for {@code initialRetryDelayMillis}.
 * Each subsequent failure the sleep interval is calculated as at least half of and no more than:
 *
 * <pre>
 *{@code initialRetryDelayMillis} * {@code retryDelayBackoffFactor} ^ NumFailures
 *</pre>
 *
 * This proceeds until either the request is successful, {@code retryMaxAttempts} are made, or both
 * {@code retryMinAttempts} are made and {@code totalRetryPeriodMillis} have elapsed.
 *
 * To construct {@code RetryParams}, first create a {@link RetryParams.Builder}. The builder is
 * mutable and each of the parameters can be set (any unset parameters will fallback to the
 * defaults). The {@code Builder} can be then used to create an immutable {@code RetryParams}
 * object.
 *
 * For default {@code RetryParams} use {@link #getDefaultInstance}. Default settings are subject to
 * change release to release. If you require specific settings, explicitly create an instance of
 * {@code RetryParams} with the required settings.
 *
 */
public class RetryParams implements Serializable {
  private static final long serialVersionUID = -8492751576749007700L;

  public static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 5000;
  public static final int DEFAULT_RETRY_MIN_ATTEMPTS = 3;
  public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 6;
  public static final long DEFAULT_INITIAL_RETRY_DELAY_MILLIS = 100;
  public static final long DEFAULT_MAX_RETRY_DELAY_MILLIS = 10 * 1000;
  public static final double DEFAULT_RETRY_DELAY_BACKOFF_FACTOR = 2;
  public static final long DEFAULT_TOTAL_RETRY_PERIOD_MILLIS = 30 * 1000;

  private final long requestTimeoutMillis;
  private final int retryMinAttempts;
  private final int retryMaxAttempts;
  private final long initialRetryDelayMillis;
  private final long maxRetryDelayMillis;
  private final double retryDelayBackoffFactor;
  private final long totalRetryPeriodMillis;

  private static final RetryParams DEFAULT_INSTANCE = new RetryParams(new Builder());


  /**
   * Create a new RetryParams with the parameters from a {@link RetryParams.Builder}
   *
   * @param builder the parameters to use to construct the RetryParams object
   */
  private RetryParams(Builder builder) {
    requestTimeoutMillis = builder.requestTimeoutMillis;
    retryMinAttempts = builder.retryMinAttempts;
    retryMaxAttempts = builder.retryMaxAttempts;
    initialRetryDelayMillis = builder.initialRetryDelayMillis;
    maxRetryDelayMillis = builder.maxRetryDelayMillis;
    retryDelayBackoffFactor = builder.retryDelayBackoffFactor;
    totalRetryPeriodMillis = builder.totalRetryPeriodMillis;
  }

  /**
   * Retrieve an instance with the default parameters
   */
  public static RetryParams getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  public static final class Builder {

    private long requestTimeoutMillis;
    private int retryMinAttempts;
    private int retryMaxAttempts;
    private long initialRetryDelayMillis;
    private long maxRetryDelayMillis;
    private double retryDelayBackoffFactor;
    private long totalRetryPeriodMillis;

    public Builder() {
      this(null);
    }

    public Builder( RetryParams retryParams) {
      if (retryParams == null) {
        requestTimeoutMillis = DEFAULT_REQUEST_TIMEOUT_MILLIS;
        retryMinAttempts = DEFAULT_RETRY_MIN_ATTEMPTS;
        retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
        initialRetryDelayMillis = DEFAULT_INITIAL_RETRY_DELAY_MILLIS;
        maxRetryDelayMillis = DEFAULT_MAX_RETRY_DELAY_MILLIS;
        retryDelayBackoffFactor = DEFAULT_RETRY_DELAY_BACKOFF_FACTOR;
        totalRetryPeriodMillis = DEFAULT_TOTAL_RETRY_PERIOD_MILLIS;
      } else {
        requestTimeoutMillis = retryParams.getRequestTimeoutMillis();
        retryMinAttempts = retryParams.getRetryMinAttempts();
        retryMaxAttempts = retryParams.getRetryMaxAttempts();
        initialRetryDelayMillis = retryParams.getInitialRetryDelayMillis();
        maxRetryDelayMillis = retryParams.getMaxRetryDelayMillis();
        retryDelayBackoffFactor = retryParams.getRetryDelayBackoffFactor();
        totalRetryPeriodMillis = retryParams.getTotalRetryPeriodMillis();
      }
    }

    /**
     * @param retryMinAttempts the retryMinAttempts to set
     * @return the Builder for chaining
     */
    public Builder retryMinAttempts(int retryMinAttempts) {
      this.retryMinAttempts = retryMinAttempts;
      return this;
    }

    /**
     * @param retryMaxAttempts the retryMaxAttempts to set
     * @return the Builder for chaining
     */
    public Builder retryMaxAttempts(int retryMaxAttempts) {
      this.retryMaxAttempts = retryMaxAttempts;
      return this;
    }

    /**
     * @param initialRetryDelayMillis the initialRetryDelayMillis to set
     * @return the Builder for chaining
     */
    public Builder initialRetryDelayMillis(long initialRetryDelayMillis) {
      this.initialRetryDelayMillis = initialRetryDelayMillis;
      return this;
    }

    /**
     * @param maxRetryDelayMillis the maxRetryDelayMillis to set
     * @return the Builder for chaining
     */
    public Builder maxRetryDelayMillis(long maxRetryDelayMillis) {
      this.maxRetryDelayMillis = maxRetryDelayMillis;
      return this;
    }

    /**
     * @param retryDelayBackoffFactor the retryDelayBackoffFactor to set
     * @return the Builder for chaining
     */
    public Builder retryDelayBackoffFactor(double retryDelayBackoffFactor) {
      this.retryDelayBackoffFactor = retryDelayBackoffFactor;
      return this;
    }

    /**
     * @param totalRetryPeriodMillis the totalRetryPeriodMillis to set
     * @return the Builder for chaining
     */
    public Builder totalRetryPeriodMillis(long totalRetryPeriodMillis) {
      this.totalRetryPeriodMillis = totalRetryPeriodMillis;
      return this;
    }

    /**
     * @param requestTimeoutMillis the requestTimeoutMillis to set
     * @return the Builder for chaining
     */
    public Builder requestTimeoutMillis(long requestTimeoutMillis) {
      this.requestTimeoutMillis = requestTimeoutMillis;
      return this;
    }

    /**
     * Create an instance of RetryParams with the parameters set in this builder
     * @return a new instance of RetryParams
     */
    public RetryParams build() {
      return new RetryParams(this);
    }
  }

  /**
   * @return the retryMinAttempts
   */
  public int getRetryMinAttempts() {
    return retryMinAttempts;
  }

  /**
   * @return the retryMaxAttempts
   */
  public int getRetryMaxAttempts() {
    return retryMaxAttempts;
  }

  /**
   * @return the initialRetryDelayMillis
   */
  public long getInitialRetryDelayMillis() {
    return initialRetryDelayMillis;
  }

  /**
   * @return the maxRetryDelayMillis
   */
  public long getMaxRetryDelayMillis() {
    return maxRetryDelayMillis;
  }

  /**
   * @return the maxRetryDelayBackoffFactor
   */
  public double getRetryDelayBackoffFactor() {
    return retryDelayBackoffFactor;
  }

  /**
   * @return the totalRetryPeriodMillis
   */
  public long getTotalRetryPeriodMillis() {
    return totalRetryPeriodMillis;
  }

  /**
   * @return the requestTimeoutMillis
   */
  public long getRequestTimeoutMillis() {
    return requestTimeoutMillis;
  }
}
