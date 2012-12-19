package com.google.appengine.tools.cloudstorage.dev;

import com.google.appengine.tools.cloudstorage.RawGcsService;

/**
 * Creates a RawGcsService for use in-process. This is useful for testing.
 */
public final class LocalRawGcsServiceFactory {

  private LocalRawGcsServiceFactory() {}
  
  public static RawGcsService createLocalRawGcsService() {
    return new LocalRawGcsService();
  }
  
}
