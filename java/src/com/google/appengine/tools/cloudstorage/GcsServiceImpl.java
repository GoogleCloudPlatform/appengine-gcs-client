package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken;
import com.google.appengine.tools.cloudstorage.RetryHelper.Body;

import java.io.IOException;
import java.nio.channels.ReadableByteChannel;

/**
 * Basic implementation of {@link GcsService}. Mostly delegates to {@link RawGcsService}
 */
final class GcsServiceImpl implements GcsService {

  private final RawGcsService raw;
  private final RetryParams retryParams;

  GcsServiceImpl(RawGcsService raw, RetryParams retryParams) {
    this.raw = checkNotNull(raw, "Null raw");
    this.retryParams = retryParams;
  }

  @Override
  public String toString() {
    return "GcsServiceImpl [retryParams=" + retryParams + "]";
  }

  @Override
  public GcsOutputChannel createOrReplace(
      final GcsFilename filename, final GcsFileOptions options) throws IOException {
    RawGcsCreationToken token = RetryHelper.runWithRetries(new Body<RawGcsCreationToken>() {
      @Override
      public RawGcsCreationToken run() throws IOException {
        return raw.beginObjectCreation(filename, options, retryParams.getRequestTimeoutMillis());
      }
    }, retryParams);
    return new GcsOutputChannelImpl(raw, token, retryParams);
  }

  @Override
  public ReadableByteChannel openReadChannel(GcsFilename filename, long startPosition) {
    return new SimpleGcsInputChannelImpl(raw, filename, startPosition, retryParams);
  }

  @Override
  public ReadableByteChannel openPrefetchingReadChannel(
      GcsFilename filename, long startPosition, int blockSize) {
    return new PrefetchingGcsInputChannelImpl(
        raw, filename, blockSize, startPosition, retryParams);
  }

  @Override
  public GcsFileMetadata getMetadata(final GcsFilename filename) throws IOException {
    return RetryHelper.runWithRetries(new Body<GcsFileMetadata>() {
      @Override
      public GcsFileMetadata run() throws IOException {
        return raw.getObjectMetadata(filename, retryParams.getRequestTimeoutMillis());
      }
    }, retryParams);
  }

  @Override
  public boolean delete(final GcsFilename filename) throws IOException {
    return RetryHelper.runWithRetries(new Body<Boolean>() {
      @Override
      public Boolean run() throws IOException {
        return raw.deleteObject(filename, retryParams.getRequestTimeoutMillis());
      }
    }, retryParams);
  }
}
