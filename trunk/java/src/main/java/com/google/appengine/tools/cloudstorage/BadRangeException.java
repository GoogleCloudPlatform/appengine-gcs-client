package com.google.appengine.tools.cloudstorage;


/**
 * Thrown from RawGcsService when a read operation begins at an offset that is >= the length of the
 * file. This is caught by GcsInputChannel.
 */
public final class BadRangeException extends RuntimeException {
  private static final long serialVersionUID = 780199686075408083L;

  BadRangeException() {
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
