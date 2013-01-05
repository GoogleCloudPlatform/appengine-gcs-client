package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import com.google.appengine.tools.cloudstorage.RetryHelper.Body;
import com.google.appengine.tools.cloudstorage.RetryHelper.RetryInteruptedException;
import com.google.common.base.Preconditions;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An implementation of {@link ReadableByteChannel} than attempts to load the data into memory
 * before it is actually needed to avoid blocking the calling thread.
 *
 */
final class PrefetchingGcsInputChannelImpl implements ReadableByteChannel {

  private static final Logger log =
      Logger.getLogger(PrefetchingGcsInputChannelImpl.class.getName());

  private final Object lock = new Object();
  private final RawGcsService raw;
  private final GcsFilename filename;
  private final int blockSizeBytes;

  private boolean closed = false;
  private boolean eofHit = false;

  private long fetchPosition;
  private Future<GcsFileMetadata> pendingFetch = null;
  private ByteBuffer current = ByteBuffer.allocate(0);
  private ByteBuffer next;

  private final RetryParams retryParams;

  PrefetchingGcsInputChannelImpl(RawGcsService raw, GcsFilename filename, int blockSizeBytes,
      long startPosition, RetryParams retryParams) {
    this.raw = checkNotNull(raw, "Null raw");
    this.filename = checkNotNull(filename, "Null filename");
    checkState(blockSizeBytes >= 1024, "Block size must be at least 1kb. Was: " + blockSizeBytes);
    this.blockSizeBytes = blockSizeBytes;
    this.retryParams = retryParams;
    this.fetchPosition = startPosition;
    this.next = ByteBuffer.allocate(blockSizeBytes);
    this.pendingFetch =
        raw.readObjectAsync(next, filename, fetchPosition, retryParams.getRequestTimeoutMillis());
  }

  @Override
  public String toString() {
    return "PrefetchingGcsInputChannelImpl [filename=" + filename + ", blockSizeBytes="
        + blockSizeBytes + ", closed=" + closed + ", eofHit=" + eofHit + ", fetchPosition="
        + fetchPosition + ", pendingFetch=" + pendingFetch + ", retryParams=" + retryParams + "]";
  }


  @Override
  public boolean isOpen() {
    synchronized (lock) {
      return !closed;
    }
  }

  @Override
  public void close() {
    synchronized (lock) {
      closed = true;
    }
  }

  private void waitForFetchWithRetry() throws IOException {
    try {
      RetryHelper.runWithRetries(new Body<Void>() {
        @Override
        public Void run() throws IOException {
          waitForFetch();
          return null;
        }}, retryParams);
    } catch (RetryInteruptedException e) {
      closed = true;
      throw new ClosedByInterruptException();
    }
  }

  private void waitForFetch() throws IOException {
    Preconditions.checkState(pendingFetch != null, "%s: no fetch pending", this);
    Preconditions.checkState(!current.hasRemaining(), "%s: current has remaining", this);
    try {
      GcsFileMetadata gcsFileMetadata = pendingFetch.get();
      flipToNextBlockAndPrefetch(gcsFileMetadata.getLength());
    } catch (ExecutionException e) {
      if (e.getCause() instanceof BadRangeException) {
        eofHit = true;
        current = null;
        next = null;
        pendingFetch = null;
      } else if (e.getCause() instanceof FileNotFoundException) {
        FileNotFoundException toThrow = new FileNotFoundException(e.getMessage());
        toThrow.initCause(e);
        throw toThrow;
      } else if (e.getCause() instanceof IOException) {
        log.log(Level.WARNING, this + ": IOException fetching block", e);
        next = ByteBuffer.allocate(blockSizeBytes);
        pendingFetch = raw.readObjectAsync(
            next, filename, fetchPosition, retryParams.getRequestTimeoutMillis());
        throw new IOException(this + ": Prefetch failed, prefetching again", e);
      } else {
        throw new RuntimeException(this + ": Unexpected cause of ExecutionException", e);
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      closed = true;
      throw new ClosedByInterruptException();
    }
  }

  private void flipToNextBlockAndPrefetch(long contentLength) {
    Preconditions.checkState(next != null, "%s: no next", this);
    current = next;
    current.flip();
    fetchPosition += blockSizeBytes;

    if (fetchPosition >= contentLength) {
      eofHit = true;
      next = null;
      pendingFetch = null;
    } else {
      next = ByteBuffer.allocate(blockSizeBytes);
      pendingFetch =
          raw.readObjectAsync(next, filename, fetchPosition, retryParams.getRequestTimeoutMillis());
    }
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    synchronized (lock) {
      if (closed) {
        throw new ClosedChannelException();
      }
      if (eofHit && !current.hasRemaining()) {
        return -1;
      }
      Preconditions.checkArgument(dst.remaining() > 0, "Requested to read data into a full buffer");
      if (!current.hasRemaining()) {
        waitForFetchWithRetry();
      }
      Preconditions.checkState(current.hasRemaining(), "%s: no remaining after wait", this);
      int toRead = dst.remaining();
      if (current.remaining() <= toRead) {
        dst.put(current);
        if (pendingFetch != null && pendingFetch.isDone()) {
          waitForFetchWithRetry();
        }
        return toRead - dst.remaining();
      } else {
        int oldLimit = current.limit();
        current.limit(current.position() + toRead);
        dst.put(current);
        current.limit(oldLimit);
        return toRead;
      }
    }
  }

}
