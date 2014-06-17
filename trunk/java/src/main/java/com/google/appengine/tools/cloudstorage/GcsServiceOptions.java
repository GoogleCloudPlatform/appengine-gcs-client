package com.google.appengine.tools.cloudstorage;

import java.util.Objects;


/**
 * A class to hold options for the GcsService
 */
public class GcsServiceOptions {

  private final RetryParams retryParams;
  private final Integer defaultWriteBufferSize;

  public static class Builder {
    private RetryParams retryParams;
    private Integer defaultWriteBufferSize;

    public Builder withRetryParams(RetryParams retryParams) {
      this.retryParams = retryParams;
      return this;
    }

    /**
     * @param defaultWriteBufferSize the size of the default write buffer in bytes
     */
    public Builder withDefaultWriteBufferSize(Integer defaultWriteBufferSize) {
      this.defaultWriteBufferSize = defaultWriteBufferSize;
      return this;
    }

    public GcsServiceOptions build() {
      return new GcsServiceOptions(retryParams, defaultWriteBufferSize);
    }
  }

  private GcsServiceOptions(RetryParams retryParams, Integer defaultWriteBufferSize) {
    if (retryParams == null) {
      this.retryParams = RetryParams.getDefaultInstance();
    } else {
      this.retryParams = retryParams;
    }
    this.defaultWriteBufferSize = defaultWriteBufferSize;
  }

  public RetryParams getRetryParams() {
    return retryParams;
  }

  public Integer getDefaultWriteBufferSize() {
    return defaultWriteBufferSize;
  }

  @Override
  public int hashCode() {
    return Objects.hash(retryParams, defaultWriteBufferSize);
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
        && Objects.equals(defaultWriteBufferSize, other.defaultWriteBufferSize);
  }

  @Override
  public String toString() {
    return "GcsServiceOptions [retryParams=" + retryParams + ", defaultWriteBufferSize="
        + defaultWriteBufferSize + "]";
  }
}
