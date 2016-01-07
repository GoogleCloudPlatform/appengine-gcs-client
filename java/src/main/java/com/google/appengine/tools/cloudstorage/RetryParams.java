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

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;
import java.util.Objects;

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
public final class RetryParams implements Serializable {
  private static final long serialVersionUID = -8492751576749007700L;

  public static final long DEFAULT_REQUEST_TIMEOUT_MILLIS = 30_000L;
  public static final double DEFAULT_REQUEST_TIMEOUT_RETRY_FACTOR = 1.2;
  public static final long DEFAULT_MAX_REQUEST_TIMEOUT = 2L * DEFAULT_REQUEST_TIMEOUT_MILLIS;
  public static final int DEFAULT_RETRY_MIN_ATTEMPTS = 3;
  public static final int DEFAULT_RETRY_MAX_ATTEMPTS = 6;
  public static final long DEFAULT_INITIAL_RETRY_DELAY_MILLIS = 1000L;
  public static final long DEFAULT_MAX_RETRY_DELAY_MILLIS = 32_000L;
  public static final double DEFAULT_RETRY_DELAY_BACKOFF_FACTOR = 2.0;
  public static final long DEFAULT_TOTAL_RETRY_PERIOD_MILLIS = 50_000L;

  private final long requestTimeoutMillis;
  private final double requestTimeoutRetryFactor;
  private final long maxRequestTimeout;
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
    requestTimeoutRetryFactor = builder.requestTimeoutRetryFactor;
    maxRequestTimeout = builder.maxRequestTimeout;
    retryMinAttempts = builder.retryMinAttempts;
    retryMaxAttempts = builder.retryMaxAttempts;
    initialRetryDelayMillis = builder.initialRetryDelayMillis;
    maxRetryDelayMillis = builder.maxRetryDelayMillis;
    retryDelayBackoffFactor = builder.retryDelayBackoffFactor;
    totalRetryPeriodMillis = builder.totalRetryPeriodMillis;
    checkArgument(requestTimeoutMillis >= 0, "requestTimeoutMillis must not be negative");
    checkArgument(requestTimeoutRetryFactor >= 0, "requestTimeoutRetryFactor must not be negative");
    checkArgument(maxRequestTimeout >= requestTimeoutMillis,
        "maxRequestTimeout must not be smaller than requestTimeoutMillis");
    checkArgument(retryMinAttempts >= 0, "retryMinAttempts must not be negative");
    checkArgument(retryMaxAttempts >= retryMinAttempts,
        "retryMaxAttempts must not be smaller than retryMinAttempts");
    checkArgument(initialRetryDelayMillis >= 0, "initialRetryDelayMillis must not be negative");
    checkArgument(maxRetryDelayMillis >= initialRetryDelayMillis,
        "maxRetryDelayMillis must not be smaller than initialRetryDelayMillis");
    checkArgument(retryDelayBackoffFactor >= 0, "retryDelayBackoffFactor must not be negative");
    checkArgument(totalRetryPeriodMillis >= 0, "totalRetryPeriodMillis must not be negative");
  }

  /**
   * Retrieve an instance with the default parameters
   */
  public static RetryParams getDefaultInstance() {
    return DEFAULT_INSTANCE;
  }

  /**
   * RetryParams builder.
   */
  public static final class Builder {

    private long requestTimeoutMillis;
    private double requestTimeoutRetryFactor;
    private long maxRequestTimeout;
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
        requestTimeoutRetryFactor = DEFAULT_REQUEST_TIMEOUT_RETRY_FACTOR;
        maxRequestTimeout = DEFAULT_MAX_REQUEST_TIMEOUT;
        retryMinAttempts = DEFAULT_RETRY_MIN_ATTEMPTS;
        retryMaxAttempts = DEFAULT_RETRY_MAX_ATTEMPTS;
        initialRetryDelayMillis = DEFAULT_INITIAL_RETRY_DELAY_MILLIS;
        maxRetryDelayMillis = DEFAULT_MAX_RETRY_DELAY_MILLIS;
        retryDelayBackoffFactor = DEFAULT_RETRY_DELAY_BACKOFF_FACTOR;
        totalRetryPeriodMillis = DEFAULT_TOTAL_RETRY_PERIOD_MILLIS;
      } else {
        requestTimeoutMillis = retryParams.getRequestTimeoutMillis();
        requestTimeoutRetryFactor = retryParams.getRequestTimeoutRetryFactor();
        maxRequestTimeout = retryParams.getMaxRequestTimeout();
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
     * @param requestTimeoutRetryFactor the requestTimeoutRetryFactor to set
     * @return the Builder for chaining
     */
    Builder requestTimeoutRetryFactor(double requestTimeoutRetryFactor) {
      this.requestTimeoutRetryFactor = requestTimeoutRetryFactor;
      return this;
    }

    /**
     * @param maxRequestTimeout the maxRequestTimeout to set
     * @return the Builder for chaining
     */
    Builder maxRequestTimeout(long maxRequestTimeout) {
      this.maxRequestTimeout = maxRequestTimeout;
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

  /**
   * Returns a request-timeout in milliseconds for the current attempt.
   * The returned value is based on the initial-value * back-off ^ (attempt -1)
   * but bounded by the max-value. Identical to {@link #getRequestTimeoutMillis}
   * if {@link RetryHelper.Context} is not available.
   */
  long getRequestTimeoutMillisForCurrentAttempt() {
    RetryHelper.Context context = RetryHelper.getContext();
    if (context == null) {
      return getRequestTimeoutMillis();
    }
    int attempt = context.getAttemptNumber();
    return getExponentialValue(
        requestTimeoutMillis, requestTimeoutRetryFactor, maxRequestTimeout, attempt);
  }

  static long getExponentialValue(long initialValue, double factorValue, long maxValue,
      int attemptsSoFar) {
    if (attemptsSoFar <= 1) {
      attemptsSoFar = 1;
    }
    return (long) Math.min(maxValue, Math.pow(factorValue, attemptsSoFar - 1) * initialValue);
  }

  /**
   * @return the requestTimeoutRetryFactor
   */
  double getRequestTimeoutRetryFactor() {
    return requestTimeoutRetryFactor;
  }

  /**
   * @return the maxRequestTimeout
   */
  long getMaxRequestTimeout() {
    return maxRequestTimeout;
  }

  @Override
  public int hashCode() {
    return Objects.hash(requestTimeoutMillis, requestTimeoutRetryFactor, maxRequestTimeout,
        retryMinAttempts, retryMaxAttempts, initialRetryDelayMillis, maxRetryDelayMillis,
        retryDelayBackoffFactor, totalRetryPeriodMillis);
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (!(obj instanceof RetryParams)) {
      return false;
    }
    RetryParams other = (RetryParams) obj;
    return requestTimeoutMillis == other.requestTimeoutMillis
        && requestTimeoutRetryFactor == other.requestTimeoutRetryFactor
        && maxRequestTimeout == other.maxRequestTimeout
        && retryMinAttempts == other.retryMinAttempts
        && retryMaxAttempts == other.retryMaxAttempts
        && initialRetryDelayMillis == other.initialRetryDelayMillis
        && maxRetryDelayMillis == other.maxRetryDelayMillis
        && retryDelayBackoffFactor == other.retryDelayBackoffFactor
        && totalRetryPeriodMillis == other.totalRetryPeriodMillis;
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + " [requestTimeoutMillis=" + requestTimeoutMillis
        + ", requestTimeoutRetryFactor=" + requestTimeoutRetryFactor
        + ", maxRequestTimeout=" + maxRequestTimeout
        + ", retryMinAttempts=" + retryMinAttempts + ", retryMaxAttempts=" + retryMaxAttempts
        + ", initialRetryDelayMillis=" + initialRetryDelayMillis + ", maxRetryDelayMillis="
        + maxRetryDelayMillis + ", retryDelayBackoffFactor=" + retryDelayBackoffFactor
        + ", totalRetryPeriodMillis=" + totalRetryPeriodMillis + "]";
  }
}
