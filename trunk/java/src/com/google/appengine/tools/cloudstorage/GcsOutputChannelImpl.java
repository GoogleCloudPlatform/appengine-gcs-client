package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken;
import com.google.appengine.tools.cloudstorage.RetryHelper.Body;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.logging.Logger;

final class GcsOutputChannelImpl implements GcsOutputChannel {

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(GcsOutputChannelImpl.class.getName());

  private final Object lock = new Object();
  private final ByteBuffer buf;
  private final RawGcsService raw;private RawGcsCreationToken token;
  private final GcsFilename filename;
  private RetryParams retryParams;


  GcsOutputChannelImpl(RawGcsService raw, RawGcsCreationToken nextToken, RetryParams retryParams) {
    this.retryParams = retryParams;
    this.raw = checkNotNull(raw, "Null raw");
    this.buf = ByteBuffer.allocate(getBufferSize(raw.getChunkSizeBytes()));
    this.token = checkNotNull(nextToken, "Null token");
    this.filename = nextToken.getFilename();
  }

  private int getBufferSize(int chunkSize) {
    if (chunkSize <= 256 * 1024) {
      return 8 * chunkSize;
    } else if (chunkSize <= 1024 * 1024) {
      return 2 * chunkSize;
    } else {
      return chunkSize;
    }
  }

  @Override
  public int getBufferSizeBytes() {
    return buf.capacity();
  }

  @Override
  public String toString() {
    return "GcsOutputChannelImpl [token=" + token + ", filename=" + filename
        + ", retryParams=" + retryParams + "]";
  }

  @Override
  public boolean isOpen() {
    synchronized (lock) {
      return token != null;
    }
  }

  @Override
  public GcsFilename getFilename() {
    return filename;
  }

  private ByteBuffer getSliceForWrite() {
    int oldPos = buf.position();
    buf.flip();
    ByteBuffer out = buf.slice();
    buf.limit(buf.capacity());
    buf.position(oldPos);
    return out;
  }

  @Override
  public void close() throws IOException {
    synchronized (lock) {
      if (!isOpen()) {
        return;
      }
      final ByteBuffer out = getSliceForWrite();
      RetryHelper.runWithRetries(new Body<Void>() {
        @Override
        public Void run() throws IOException {
          raw.finishObjectCreation(token, out, retryParams.getRequestTimeoutMillis());
          return null;
        }
      }, retryParams);
      token = null;
    }
  }

  private void flushIfNeeded() throws IOException {
    if (buf.hasRemaining()) {
      return;
    }
    Preconditions.checkState(!buf.hasRemaining(), "%s: %s", this, buf);
    final ByteBuffer out = getSliceForWrite();
    try {
      token = RetryHelper.runWithRetries(new Body<RawGcsCreationToken>() {
        @Override
        public RawGcsCreationToken run() throws IOException {
          return raw.continueObjectCreation(token, out, retryParams.getRequestTimeoutMillis());
        }}, retryParams);
      buf.clear();
    } catch (ClosedByInterruptException e) {
      token = null;
      throw new ClosedByInterruptException();
    }
  }

  @Override
  public int write(ByteBuffer in) throws IOException {
    synchronized (lock) {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      int inBufferSize = in.remaining();
      while (in.hasRemaining()) {
        flushIfNeeded();
        Preconditions.checkState(buf.hasRemaining(), "%s: %s", this, buf);
        int numBytesToCopyToBuffer = Math.min(buf.remaining(), in.remaining());

        int oldLimit = in.limit();
        in.limit(in.position() + numBytesToCopyToBuffer);
        buf.put(in);
        in.limit(oldLimit);
      }
      flushIfNeeded();
      return inBufferSize;
    }
  }

}
