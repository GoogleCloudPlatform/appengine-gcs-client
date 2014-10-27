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

import com.google.appengine.tools.cloudstorage.RawGcsService.ListItemBatch;
import com.google.appengine.tools.cloudstorage.RawGcsService.RawGcsCreationToken;
import com.google.apphosting.api.ApiProxy.ApiDeadlineExceededException;
import com.google.apphosting.api.ApiProxy.OverQuotaException;
import com.google.apphosting.api.ApiProxy.RPCFailedException;
import com.google.apphosting.api.ApiProxy.UnknownException;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.MalformedURLException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.util.Iterator;
import java.util.concurrent.Callable;

/**
 * Basic implementation of {@link GcsService}. Mostly delegates to {@link RawGcsService}
 */
final class GcsServiceImpl implements GcsService {

  private final RawGcsService raw;
  private final GcsServiceOptions options;
  private static final int MAX_RESULTS_PER_BATCH = 100;
  static final ExceptionHandler exceptionHandler = new ExceptionHandler.Builder()
      .retryOn(UnknownException.class, RPCFailedException.class, ApiDeadlineExceededException.class,
          IOException.class, SocketTimeoutException.class, OverQuotaException.class)
      .abortOn(InterruptedException.class, FileNotFoundException.class,
          MalformedURLException.class, ClosedByInterruptException.class,
          InterruptedIOException.class)
      .build();

  GcsServiceImpl(RawGcsService raw, GcsServiceOptions options) {
    this.raw = checkNotNull(raw, "Null raw");
    this.options = options;
  }

  @Override
  public String toString() {
    return "GcsServiceImpl [serviceOptions=" + options + "]";
  }

  @Override
  public GcsOutputChannel createOrReplace(
      final GcsFilename filename, final GcsFileOptions fileOptions) throws IOException {
    try {
      RawGcsCreationToken token = RetryHelper.runWithRetries(new Callable<RawGcsCreationToken>() {
        @Override
        public RawGcsCreationToken call() throws IOException {
          long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
          return raw.beginObjectCreation(filename, fileOptions, timeout);
        }
      }, options.getRetryParams(), exceptionHandler);
      return new GcsOutputChannelImpl(
          raw, token, options.getRetryParams(), options.getDefaultWriteBufferSize(),
          options.getHttpHeaders());
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public void createOrReplace(final GcsFilename filename, final GcsFileOptions fileOptions,
      final ByteBuffer src) throws IOException {
    if (src.remaining() > raw.getMaxWriteSizeByte()) {
      @SuppressWarnings("resource")
      GcsOutputChannel channel = createOrReplace(filename, fileOptions);
      channel.write(src);
      channel.close();
      return;
    }

    try {
      RetryHelper.runWithRetries(new Callable<Void>() {
          @Override
          public Void call() throws IOException {
            long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
            raw.putObject(filename, fileOptions, src, timeout);
            return null;
          }
        }, options.getRetryParams(), exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public GcsInputChannel openReadChannel(GcsFilename filename, long startPosition) {
    return new SimpleGcsInputChannelImpl(raw, filename, startPosition, options.getRetryParams(),
        options.getHttpHeaders());
  }

  @Override
  public GcsInputChannel openPrefetchingReadChannel(
      GcsFilename filename, long startPosition, int blockSize) {
    return new PrefetchingGcsInputChannelImpl(raw, filename, blockSize, startPosition,
        options.getRetryParams(), options.getHttpHeaders());
  }

  @Override
  public GcsFileMetadata getMetadata(final GcsFilename filename) throws IOException {
    try {
      return RetryHelper.runWithRetries(new Callable<GcsFileMetadata>() {
        @Override
        public GcsFileMetadata call() throws IOException {
          long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
          return raw.getObjectMetadata(filename, timeout);
        }
      }, options.getRetryParams(), exceptionHandler);
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
          long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
          return raw.deleteObject(filename, timeout);
        }
      }, options.getRetryParams(), exceptionHandler);
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
      RetryHelper.runWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
          raw.composeObject(source, dest, timeout);
          return null;
        }
      }, options.getRetryParams(), exceptionHandler);
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
      RetryHelper.runWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
          raw.copyObject(source, dest, null, timeout);
          return null;
        }
      }, options.getRetryParams(), exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }

  @Override
  public ListResult list(final String bucket, ListOptions listOptions) throws IOException {
    if (listOptions == null) {
      listOptions = ListOptions.DEFAULT;
    }
    final String prefix = listOptions.getPrefix();
    final String delimiter = listOptions.isRecursive() ? null : options.getPathDelimiter();
    Callable<Iterator<ListItem>> batcher = new Callable<Iterator<ListItem>>() {
      private String nextMarker = "";

      @Override
      public Iterator<ListItem> call() throws IOException {
        if (nextMarker == null) {
          return null;
        }
        ListItemBatch batch;
        try {
          batch = RetryHelper.runWithRetries(new Callable<ListItemBatch>() {
            @Override
            public ListItemBatch call() throws IOException {
              long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
              String marker = Strings.emptyToNull(nextMarker);
              return raw.list(bucket, prefix, delimiter, marker, MAX_RESULTS_PER_BATCH, timeout);
            }
          }, options.getRetryParams(), exceptionHandler);
        } catch (RetryInterruptedException ex) {
          throw new ClosedByInterruptException();
        } catch (NonRetriableException e) {
          Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
          throw e;
        }
        nextMarker = batch.getNextMarker();
        return batch.getItems().iterator();
      }
    };
    return new ListResult(batcher);
  }

  @Override
  public void update(final GcsFilename source, final GcsFileOptions fileOptions)
      throws IOException {
    try {
      RetryHelper.runWithRetries(new Callable<Void>() {
        @Override
        public Void call() throws IOException {
          long timeout = options.getRetryParams().getRequestTimeoutMillisForCurrentAttempt();
          raw.copyObject(source, source, fileOptions, timeout);
          return null;
        }
      }, options.getRetryParams(), exceptionHandler);
    } catch (RetryInterruptedException ex) {
      throw new ClosedByInterruptException();
    } catch (NonRetriableException e) {
      Throwables.propagateIfInstanceOf(e.getCause(), IOException.class);
      throw e;
    }
  }
}
