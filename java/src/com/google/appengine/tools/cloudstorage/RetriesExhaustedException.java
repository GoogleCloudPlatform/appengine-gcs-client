package com.google.appengine.tools.cloudstorage;

import java.io.IOException;

/**
 * Thrown when a RetryHelper has attempted the maximum number of attempts allowed by RetryParams
 * and was not successful.
 */
public final class RetriesExhaustedException extends IOException {
  private static final long serialVersionUID = 780199686075408083L;

  public RetriesExhaustedException() {
  }

  public RetriesExhaustedException(String message) {
    super(message);
  }

  RetriesExhaustedException(Throwable cause) {
    super(cause);
  }

  RetriesExhaustedException(String message, Throwable cause) {
    super(message, cause);
  }
}
