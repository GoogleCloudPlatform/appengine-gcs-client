package com.google.appengine.tools.cloudstorage;

import static com.google.appengine.tools.cloudstorage.RetryParams.DEFAULT_REQUEST_TIMEOUT_RETRY_FACTOR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Strings.isNullOrEmpty;

import com.google.common.collect.ImmutableMap;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;


/**
 * A class to hold options for the GcsService.
 */
public class GcsServiceOptions implements Serializable {

  private static final long serialVersionUID = 6595783378343634936L;
  public static final GcsServiceOptions DEFAULT = new Builder().build();

  private final RetryParams retryParams;
  private final Integer defaultWriteBufferSize;
  private final String pathDelimiter;
  private final Map<String, String> headers;

  /**
   * GcsServiceOtpions builder.
   */
  public static final class Builder {

    private RetryParams retryParams;
    private Integer defaultWriteBufferSize;
    private String pathDelimiter = "/";
    private Map<String, String> headers = ImmutableMap.of();

    public Builder setRetryParams(RetryParams retryParams) {
      this.retryParams = retryParams;
      return this;
    }

    /**
     * @param defaultWriteBufferSize the size of the default write buffer in bytes
     */
    public Builder setDefaultWriteBufferSize(Integer defaultWriteBufferSize) {
      this.defaultWriteBufferSize = defaultWriteBufferSize;
      return this;
    }

    public Builder setPathDelimiter(String pathDelimiter) {
      checkArgument(isNullOrEmpty(pathDelimiter), "pathDelimiter must not be null or empty");
      this.pathDelimiter = pathDelimiter;
      return this;
    }

    public Builder setHttpHeaders(Map<String, String> headers) {
      this.headers = ImmutableMap.copyOf(checkNotNull(headers));
      return this;
    }

    public GcsServiceOptions build() {
      return new GcsServiceOptions(this);
    }
  }

  private GcsServiceOptions(Builder builder) {
    retryParams = new RetryParams.Builder(
        firstNonNull(builder.retryParams, RetryParams.getDefaultInstance()))
        .requestTimeoutRetryFactor(DEFAULT_REQUEST_TIMEOUT_RETRY_FACTOR)
        .build();
    defaultWriteBufferSize = builder.defaultWriteBufferSize;
    pathDelimiter = builder.pathDelimiter;
    headers = builder.headers;
  }

  private static <T> T firstNonNull(T v1, T v2) {
    return v1 != null ? v1 : v2;
  }

  public RetryParams getRetryParams() {
    return retryParams;
  }

  public Integer getDefaultWriteBufferSize() {
    return defaultWriteBufferSize;
  }

  public String getPathDelimiter() {
    return pathDelimiter;
  }

  public Map<String, String> getHttpHeaders() {
    return headers;
  }

  @Override
  public int hashCode() {
    return Objects.hash(retryParams, defaultWriteBufferSize, pathDelimiter, headers);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    GcsServiceOptions other = (GcsServiceOptions) obj;
    return Objects.equals(retryParams, other.retryParams)
        && Objects.equals(defaultWriteBufferSize, other.defaultWriteBufferSize)
        && Objects.equals(pathDelimiter, other.pathDelimiter)
        && Objects.equals(headers, other.headers);
  }

  @Override
  public String toString() {
    return "GcsServiceOptions [retryParams=" + retryParams + ", defaultWriteBufferSize="
        + defaultWriteBufferSize + ", pathDelimiter=" + pathDelimiter + ", headers="
        + headers + "]";
  }
}
