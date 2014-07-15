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
import static java.lang.Math.max;
import static java.lang.Math.min;

import com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Throwables;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.util.Map;
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
final class GcsOutputChannelImpl implements GcsOutputChannel {

  private static final long serialVersionUID = 3011935384698648440L;
  private static final Logger log = Logger.getLogger(GcsOutputChannelImpl.class.getName());
  private static final ByteBuffer EMPTY_BYTE_BUFFER = ByteBuffer.allocate(0);

  /**
   * Represents a request that is currently in-flight. Contains all the information needed to retry
   * the request, as well as the future for its completion.
   */
  private class OutstandingRequest {
    private final RawGcsCreationToken requestToken;
    /** buffer that is associated with the on outstanding request */
    private final ByteBuffer toWrite;
    private Future<RawGcsCreationToken> nextToken;

    OutstandingRequest(RawGcsCreationToken token, ByteBuffer toWrite) {
      this.toWrite = toWrite;
      this.requestToken = token;
      this.nextToken = raw.continueObjectCreationAsync(
          token, toWrite.slice(), retryParams.getRequestTimeoutMillisForCurrentAttempt());
    }


    RawGcsCreationToken waitForNextToken() throws IOException, InterruptedException {
      try {
        return nextToken.get();
      } catch (ExecutionException e) {
        Throwable cause = e.getCause();
        if (cause instanceof IOException) {
          log.log(Level.WARNING, this + ": IOException writing block", cause);
          throw (IOException) cause;
        } else {
          throw new RuntimeException(this + ": Unexpected cause of ExecutionException", cause);
        }
      }
    }

    void retry() {
      nextToken = raw.continueObjectCreationAsync(
          requestToken, toWrite.slice(), retryParams.getRequestTimeoutMillisForCurrentAttempt());
    }
  }

  /**
   * Held over write and close for the entire call. Per Channel specification.
   */
  private transient Object lock = new Object();
  @VisibleForTesting transient ByteBuffer buf;
  private transient RawGcsService raw;
  private transient OutstandingRequest outstandingRequest;
  private RawGcsCreationToken token;
  private final GcsFilename filename;
  private final RetryParams retryParams;
  private final Integer requestedBufferSize;
  private final Map<String, String> headers;


  GcsOutputChannelImpl(RawGcsService raw, RawGcsCreationToken nextToken, RetryParams retryParams,
      Integer requestedBufferSize, Map<String, String> headers) {
    this.retryParams = retryParams;
    this.raw = checkNotNull(raw, "Null raw");
    this.token = checkNotNull(nextToken, "Null token");
    this.filename = nextToken.getFilename();
    this.buf = EMPTY_BYTE_BUFFER;
    this.requestedBufferSize = requestedBufferSize;
    this.headers = headers;
  }

  private void readObject(ObjectInputStream aInputStream)
      throws ClassNotFoundException, IOException {
    aInputStream.defaultReadObject();
    lock = new Object();
    raw = GcsServiceFactory.createRawGcsService(headers);
    if (token != null) {
      int length = aInputStream.readInt();
      if (length > 0) {
        int bufferSize = getBufferSizeBytes();
        if (length > bufferSize) {
          throw new IllegalStateException(
              "Size of buffer " + bufferSize + " is smaller than initial contents: " + length);
        }
        byte[] initialBuffer = new byte[bufferSize];
        DataInputStream dis = new DataInputStream(aInputStream);
        dis.readFully(initialBuffer, 0, length);
        buf = ByteBuffer.wrap(initialBuffer);
        buf.position(length);
      } else {
        buf = EMPTY_BYTE_BUFFER;
      }
    }
  }

  private void writeObject(ObjectOutputStream aOutputStream) throws IOException {
    aOutputStream.defaultWriteObject();
    synchronized (lock) {
      if (token != null) {
        int length = buf.position();
        aOutputStream.writeInt(length);
        if (length > 0) {
          buf.rewind();
          byte[] toWrite = new byte[length];
          buf.get(toWrite);
          aOutputStream.write(toWrite);
        }
      }
    }
  }

  @Override
  public int getBufferSizeBytes() {
    if (requestedBufferSize == null) {
      return findBufferSize(raw.getChunkSizeBytes() * 8);
    } else {
      return findBufferSize(requestedBufferSize);
    }
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

  @Override
  public void close() throws IOException {
    synchronized (lock) {
      if (!isOpen()) {
        return;
      }
      waitForOutstandingRequest();
      buf.flip();
      try {
        RetryHelper.runWithRetries(new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            raw.finishObjectCreation(
                token, buf.slice(), retryParams.getRequestTimeoutMillisForCurrentAttempt());
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
      buf = null;
    }
  }

  /**
   * Waits for the current outstanding request retrying it with exponential backoff if it fails.
   *
   * @throws ClosedByInterruptException if request was interrupted
   * @throws IOException In the event of FileNotFoundException, MalformedURLException
   * @throws RetriesExhaustedException if exceeding the number of retries
   */
  private void waitForOutstandingRequest() throws IOException {
    if (outstandingRequest == null) {
      return;
    }
    try {
      RetryHelper.runWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws IOException, InterruptedException {
          if (RetryHelper.getContext().getAttemptNumber() > 1) {
            outstandingRequest.retry();
          }
          token = outstandingRequest.waitForNextToken();
          outstandingRequest = null;
          return null;
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

  @Override
  public int write(ByteBuffer in) throws IOException {
    int written = in.remaining();
    synchronized (lock) {
      if (!isOpen()) {
        throw new ClosedChannelException();
      }
      while (in.hasRemaining()) {
        extendBufferIfNeeded(in.remaining());
        if (in.remaining() < buf.remaining()) {
          buf.put(in);
        } else {
          int oldLimit = in.limit();
          in.limit(in.position() + buf.remaining());
          buf.put(in);
          in.limit(oldLimit);
          flushBuffer(in.remaining());
        }
      }
    }
    return written;
  }

  private void flushBuffer(int nextBytesToAdd) throws IOException {
    int chunkSize = raw.getChunkSizeBytes();
    int position = buf.position();
    int bytesToWrite = (position / chunkSize) * chunkSize;
    if (bytesToWrite > 0) {
      buf.flip();
      ByteBuffer toWrite = buf.slice();
      toWrite.limit(bytesToWrite);
      waitForOutstandingRequest();
      outstandingRequest = new OutstandingRequest(token, toWrite);
      if (position > bytesToWrite || nextBytesToAdd > 0) {
        buf.position(bytesToWrite);
        buf.limit(position);
        int newBufferSize = getNewBufferSize(position - bytesToWrite + nextBytesToAdd);
        ByteBuffer newBuf = ByteBuffer.allocate(newBufferSize);
        newBuf.put(buf);
        buf = newBuf;
      } else {
        buf = EMPTY_BYTE_BUFFER;
      }
    }
  }

  private void extendBufferIfNeeded(int nextBytesToAdd) {
    if (nextBytesToAdd <= buf.remaining()) {
      return;
    }
    int newBufferSize = getNewBufferSize(buf.position() + nextBytesToAdd);
    if (newBufferSize > buf.capacity()) {
      ByteBuffer newBuf = ByteBuffer.allocate(newBufferSize);
      buf.flip();
      newBuf.put(buf);
      buf = newBuf;
    }
  }

  private int getNewBufferSize(int requestedSize) {
    return max(getBufferSizeBytes(), findBufferSize(requestedSize));
  }

  private int findBufferSize(int requestedSize) {
    int chunkSize = raw.getChunkSizeBytes();
    int bufferSize = max(chunkSize, min(requestedSize, raw.getMaxWriteSizeByte()));
    int chunks = bufferSize / chunkSize;
    return chunkSize * chunks;
  }

  @Override
  public void waitForOutstandingWrites() throws ClosedByInterruptException, IOException {
    synchronized (lock) {
      if (isOpen()) {
        flushBuffer(0);
        waitForOutstandingRequest();
      }
    }
  }
}
