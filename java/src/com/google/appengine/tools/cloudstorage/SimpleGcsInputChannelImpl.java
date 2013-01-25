package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.RetryHelper.Body;
import com.google.appengine.tools.cloudstorage.RetryHelper.RetryInteruptedException;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ReadableByteChannel;
import java.util.concurrent.ExecutionException;

final class SimpleGcsInputChannelImpl implements ReadableByteChannel, Serializable {

  private static final long serialVersionUID = -5076387489828467162L;
  private transient Object lock = new Object();
  private transient RawGcsService raw;
  private final GcsFilename filename;
  private long position;
  private boolean closed = false;
  private boolean eofHit = false;
  private final RetryParams retryParams;

  SimpleGcsInputChannelImpl(
      RawGcsService raw, GcsFilename filename, long startPosition, RetryParams retryParams) {
    this.raw = checkNotNull(raw, "Null raw");
    this.filename = checkNotNull(filename, "Null filename");
    this.position = startPosition;
    this.retryParams = retryParams;
  }

  private void readObject(ObjectInputStream aInputStream)
      throws ClassNotFoundException, IOException {
    aInputStream.defaultReadObject();
    lock = new Object();
    raw = GcsServiceFactory.createRawGcsService();
  }

  @Override
  public boolean isOpen() {
    return !closed;
  }

  @Override
  public void close() {
    closed = true;
  }

  @Override
  public int read(ByteBuffer dst) throws IOException {
    synchronized (lock) {
      if (closed) {
        throw new ClosedChannelException();
      }
      if (eofHit) {
        return -1;
      }
      int n = dst.remaining();
      Preconditions.checkArgument(n > 0, "Requested to read data into a full buffer");
      GcsFileMetadata gcsFileMetadata = readWithRetry(dst);
      int r = n - dst.remaining();
      position += r;
      if (position >= gcsFileMetadata.getLength()) {
        eofHit = true;
      }
      return r == 0 ? -1 : r;
    }
  }

  private GcsFileMetadata readWithRetry(final ByteBuffer dst) throws IOException {
    try {
      return RetryHelper.runWithRetries(new Body<GcsFileMetadata>() {
        @Override
        public GcsFileMetadata run() throws IOException {
          try {
            return raw.readObjectAsync(
                dst, filename, position, retryParams.getRequestTimeoutMillis()).get();
          } catch (ExecutionException e) {
            if (e.getCause() instanceof IOException) {
              throw (IOException) e.getCause();
            } else {
              throw new RuntimeException(this + ": Unexpected cause of ExecutionException", e);
            }
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            closed = true;
            throw new ClosedByInterruptException();
          }
        }
      }, retryParams);
    } catch (RetryInteruptedException e) {
      Thread.currentThread().interrupt();
      closed = true;
      throw new ClosedByInterruptException();
    }
  }

}
