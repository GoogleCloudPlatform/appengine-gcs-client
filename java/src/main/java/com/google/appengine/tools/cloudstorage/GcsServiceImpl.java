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

import com.google.appengine.api.urlfetch.HTTPHeader;
import com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken;
import com.google.apphosting.api.ApiProxy.ApiDeadlineExceededException;
import com.google.apphosting.api.ApiProxy.OverQuotaException;
import com.google.apphosting.api.ApiProxy.RPCFailedException;
import com.google.apphosting.api.ApiProxy.UnknownException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Basic implementation of {@link GcsService}. Mostly delegates to {@link RawGcsService}
 */
final class GcsServiceImpl implements GcsService {

  private final RawGcsService raw;
  private final Integer defaultBufferSize;
  private final RetryParams retryParams;
  static final ExceptionHandler exceptionHandler = new ExceptionHandler.Builder()
      .retryOn(UnknownException.class, RPCFailedException.class, ApiDeadlineExceededException.class,
          IOException.class, SocketTimeoutException.class, OverQuotaException.class)
      .abortOn(InterruptedException.class, FileNotFoundException.class,
          MalformedURLException.class, ClosedByInterruptException.class,
          InterruptedIOException.class)
      .build();

  GcsServiceImpl(RawGcsService raw, RetryParams retryParams, Integer defaultBufferSize) {
    this.defaultBufferSize = defaultBufferSize;
    this.raw = checkNotNull(raw, "Null raw");
    this.retryParams = new RetryParams.Builder(retryParams).requestTimeoutRetryFactor(1.2).build();
  }

  @Override
  public String toString() {
    return "GcsServiceImpl [retryParams=" + retryParams + "]";
  }

  @Override
  public GcsOutputChannel createOrReplace(
      final GcsFilename filename, final GcsFileOptions options) throws IOException {
    try {
      RawGcsCreationToken token = RetryHelper.runWithRetries(new Callable<RawGcsCreationToken>() {
        @Override
        public RawGcsCreationToken call() throws IOException {
          return raw.beginObjectCreation(
              filename, options, retryParams.getRequestTimeoutMillisForCurrentAttempt());
        }
      }, retryParams, exceptionHandler);
      return new GcsOutputChannelImpl(raw, token, retryParams, defaultBufferSize);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public void createOrReplace(final GcsFilename filename, final GcsFileOptions options,
      final ByteBuffer src) throws IOException {
    if (src.remaining() > raw.getMaxWriteSizeByte()) {
      @SuppressWarnings("resource")
      GcsOutputChannel channel = createOrReplace(filename, options);
      channel.write(src);
      channel.close();
      return;
    }

    try {
      RetryHelper.runWithRetries(new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            raw.putObject(filename, options, src, retryParams.getRequestTimeoutMillis());
            return null;
          }
        }, retryParams, exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public GcsInputChannel openReadChannel(GcsFilename filename, long startPosition) {
    return new SimpleGcsInputChannelImpl(raw, filename, startPosition, retryParams);
  }

  @Override
  public GcsInputChannel openPrefetchingReadChannel(
      GcsFilename filename, long startPosition, int blockSize) {
    return new PrefetchingGcsInputChannelImpl(
        raw, filename, blockSize, startPosition, retryParams);
  }

  @Override
  public GcsFileMetadata getMetadata(final GcsFilename filename) throws IOException {
    try {
      return RetryHelper.runWithRetries(new Callable<GcsFileMetadata>() {
        @Override
        public GcsFileMetadata call() throws IOException {
          return raw.getObjectMetadata(
              filename, retryParams.getRequestTimeoutMillisForCurrentAttempt());
        }
      }, retryParams, exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public boolean delete(final GcsFilename filename) throws IOException {
    try {
      return RetryHelper.runWithRetries(new Callable<Boolean>() {
        @Override
        public Boolean call() throws IOException {
          return raw.deleteObject(filename, retryParams.getRequestTimeoutMillisForCurrentAttempt());
        }
      }, retryParams, exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public void compose(final Iterable<String> source, final GcsFilename dest)
      throws IOException {
    try {
      final long timeout = retryParams.getRequestTimeoutMillisForCurrentAttempt();
      RetryHelper.runWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          raw.composeObject(source, dest, timeout);
          return null;
        }
      }, retryParams, exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public void copy(final GcsFilename source, final GcsFilename dest)
      throws IOException {
    try {
      final long timeout = retryParams.getRequestTimeoutMillisForCurrentAttempt();
      RetryHelper.runWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          raw.copyObject(source, dest, timeout);
          return null;
        }
      }, retryParams, exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public void setHttpHeaders(Map<String, String> headers) {
    ImmutableSet.Builder<HTTPHeader> builder = ImmutableSet.builder();
    if (headers != null) {
      for (Map.Entry<String, String> header : headers.entrySet()) {
        builder.add(new HTTPHeader(header.getKey(), header.getValue()));
      }
    }
    raw.setHttpHeaders(builder.build());
  }
}
