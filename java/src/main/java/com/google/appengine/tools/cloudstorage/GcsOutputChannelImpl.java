/*
 * Copyright 2012 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.appengine.tools.cloudstorage;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This implementation uses double buffering. This works as follows: incoming calls to write append
 * data to a buffer. Once this buffer is filled, if there are no outstanding requests, a request is
 * sent over the network asynchronously and a new buffer is allocated so more writes can occur while
 * the request is processing. If there is already an outstanding request, the thread that called
 * write blocks until this request is completed. If a request fails it is retried with exponential
 * backoff while the thread is blocked. This allows for a very simple implementation while still
 * being very fast in the happy case where the writer is slower than the network and no failures
 * occur.
 *
 * @see GcsOutputChannel
 */
final class GcsOutputChannelImpl implements GcsOutputChannel, Serializable {

  private static final long serialVersionUID = 3011935384698648440L;

  @SuppressWarnings("unused")
  private static final Logger log = Logger.getLogger(GcsOutputChannelImpl.class.getName());

  /**
   * Represents a request that is currently inflight. Contains all the information needed to retry
   * the request, as well as the future for its completion.
   */
  private class OutstandingRequest {
    private RawGcsCreationToken requestToken;
    /** buffer that is associated with the on outstanding request */
    private final ByteBuffer ongoingRequestBuf;
    private final Future<RawGcsCreationToken> nextToken;

    OutstandingRequest(RawGcsCreationToken requestToken, ByteBuffer ongoingRequestBuf,
        Future<RawGcsCreationToken> nextToken) {
      this.requestToken = requestToken;
      this.ongoingRequestBuf = ongoingRequestBuf;
      this.nextToken = nextToken;
    }
  }

  /**
   * Held over write and close for the entire call. Per Channel specification.
   */
  private transient Object lock = new Object();
  private transient ByteBuffer buf;
  private transient RawGcsService raw;
  private transient OutstandingRequest ongoingWrite;private RawGcsCreationToken token;
  private final GcsFilename filename;
  private final RetryParams retryParams;


  GcsOutputChannelImpl(RawGcsService raw, RawGcsCreationToken nextToken, RetryParams retryParams) {
    this.retryParams = retryParams;
    this.raw = checkNotNull(raw, "Null raw");
    createNewBuffer();
    this.token = checkNotNull(nextToken, "Null token");
    this.filename = nextToken.getFilename();
  }

  private void readObject(ObjectInputStream aInputStream)
      throws ClassNotFoundException, IOException {
    aInputStream.defaultReadObject();
    lock = new Object();
    raw = GcsServiceFactory.createRawGcsService();
    if (token != null) {
      createNewBuffer();
      int length = aInputStream.readInt();
      if (length > buf.capacity()) {
        throw new IllegalArgumentException(
            "Size of buffer is smaller than initial contents: " + length);
      }
      if (length > 0) {
        byte[] initialBuffer = new byte[length];
        for (int pos = 0; pos < length;) {
          pos += aInputStream.read(initialBuffer, pos, length - pos);
        }
        buf.put(initialBuffer);
      }
    }
  }

  private void createNewBuffer() {
    buf = ByteBuffer.allocate(getBufferSize(raw.getChunkSizeBytes()));
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
    int length = (buf == null) ? 0 : buf.position();
    aOutputStream.writeInt(length);
    if (length > 0 && isOpen()) {
      buf.rewind();
      byte[] toWrite = new byte[length];
      buf.get(toWrite);
      aOutputStream.write(toWrite);
    }
  }

  @VisibleForTesting
  static int getBufferSize(int chunkSize) {
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
    if (buf == null) {
      return getBufferSize(raw.getChunkSizeBytes());
    }
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
      waitForNextToken();
      final ByteBuffer out = getSliceForWrite();
      try {
        RetryHelper.runWithRetries(new Callable<Void>() {
          @Override public Void call() throws IOException {
            raw.finishObjectCreation(token, out, retryParams.getRequestTimeoutMillis());
            return null;
          }
        }, retryParams, GcsServiceImpl.exceptionHandler);
      } catch (RetryInterruptedException ex) {
        throw new ClosedByInterruptException();
      } catch (NonRetriableException e) {
        Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
        throw e;
      }
      token = null;
    }
  }

  private void flushIfNeeded() throws IOException {
    if (!buf.hasRemaining()) {
      waitForNextToken();
      ByteBuffer toWrite = getSliceForWrite();
      createNewBuffer();
      ongoingWrite = new OutstandingRequest(token, toWrite,
          raw.continueObjectCreationAsync(token, toWrite, retryParams.getRequestTimeoutMillis()));
    }
  }

  /**
   * Waits for the current outstanding request retrying it with exponential backoff if it fails.
   *
   * @throws ClosedByInterruptException if request was interrupted
   * @throws IOException In the event of FileNotFoundException, MalformedURLException
   * @throws RetriesExhaustedException if exceeding the number of retries
   */
  private void waitForNextToken() throws IOException {
    if (ongoingWrite == null) {
      return;
    }
    try {
      RetryHelper.runWithRetries(new Callable<Void>() {
        boolean failed = false;
        @Override
        public Void call() throws IOException, InterruptedException {
          if (failed) {
            retryWrite();
          }
          try {
            token = ongoingWrite.nextToken.get();
            ongoingWrite = null;
            return null;
          } catch (ExecutionException e) {
            failed = true;
            Throwable cause = e.getCause();
            if (cause instanceof IOException) {
              log.log(Level.WARNING, this + ": IOException writing block", cause);
              throw (IOException) cause;
            } else {
              throw new RuntimeException(this + ": Unexpected cause of ExecutionException", cause);
            }
          }
        }
      }, retryParams, GcsServiceImpl.exceptionHandler);
    } catch (RetryInterruptedException ex) {
      token = null;
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  private void retryWrite() throws IOException {
    ongoingWrite = new OutstandingRequest(ongoingWrite.requestToken,
        ongoingWrite.ongoingRequestBuf, raw.continueObjectCreationAsync(
            ongoingWrite.requestToken, ongoingWrite.ongoingRequestBuf,
            retryParams.getRequestTimeoutMillis()));
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

  @Override
  public void waitForOutstandingWrites() throws ClosedByInterruptException, IOException {
    synchronized (lock) {
      if (!isOpen()) {
        return;
      }
      waitForNextToken();
      int chunkSize = raw.getChunkSizeBytes();
      int position = buf.position();
      int bytesToWrite = (position / chunkSize) * chunkSize;
      if (bytesToWrite > 0) {
        ByteBuffer outputBuffer = getSliceForWrite();
        outputBuffer.limit(bytesToWrite);
        ongoingWrite = new OutstandingRequest(token, outputBuffer, raw.continueObjectCreationAsync(
            token, outputBuffer, retryParams.getRequestTimeoutMillis()));
        waitForNextToken();
        buf.position(bytesToWrite);
        buf.limit(position);
        ByteBuffer remaining = buf.slice();
        createNewBuffer();
        buf.put(remaining);
      }
    }
  }
}
