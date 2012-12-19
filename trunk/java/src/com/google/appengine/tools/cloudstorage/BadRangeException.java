package com.google.appengine.tools.cloudstorage;

import java.io.IOException;

/**
 * Thrown when a read operation begins at an offset that is >= the length of the
 * file.
 */
public final class BadRangeException extends IOException {
  private static final long serialVersionUID = 780199686075408083L;

  public BadRangeException() {
  }

  public BadRangeException(String message) {
    super(message);
  }

  BadRangeException(Throwable cause) {
    super(cause);
  }

  BadRangeException(String message, Throwable cause) {
    super(message, cause);
  }
}
