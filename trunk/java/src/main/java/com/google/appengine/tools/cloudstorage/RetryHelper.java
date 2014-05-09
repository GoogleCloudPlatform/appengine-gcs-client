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

import static com.google.appengine.tools.cloudstorage.RetryParams.getExponentialValue;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Callable;
import java.util.logging.Logger;

/**
 * Utility class for retrying operations. For more details about the parameters, see
 * {@link RetryParams}
 *
 * If the request is never successful, a {@link RetriesExhaustedException} will be thrown.
 *
 * For internal use only. User code cannot safely depend on this class.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <V> return value of the closure that is being run with retries
 */
public class RetryHelper<V>  {

  private static final Logger log = Logger.getLogger(RetryHelper.class.getName());

  private final Stopwatch stopwatch;
  private final Callable<V> callable;
  private final RetryParams params;
  private final ExceptionHandler exceptionHandler;
  private int attemptNumber;


  private static final ThreadLocal<Context> context = new ThreadLocal<>();

  static class Context {

    private final RetryHelper<?> helper;

    Context(RetryHelper<?> helper) {
      this.helper = helper;
    }

    public RetryParams getRetryParams() {
      return helper.params;
    }

    public int getAttemptNumber() {
      return helper.attemptNumber;
    }
  }

  @VisibleForTesting
  static final void setContext(Context ctx) {
    if (ctx == null) {
      context.remove();
    } else {
      context.set(ctx);
    }
  }

  static final Context getContext() {
    return context.get();
  }

  @VisibleForTesting
  RetryHelper(Callable<V> callable, RetryParams params, ExceptionHandler exceptionHandler,
      Stopwatch stopwatch) {
    this.callable = checkNotNull(callable);
    this.params = checkNotNull(params);
    this.stopwatch = checkNotNull(stopwatch);
    this.exceptionHandler = checkNotNull(exceptionHandler);
    exceptionHandler.verifyCaller(callable);
  }

  @Override
  public String toString() {
    return getClass().getSimpleName() + "(" + stopwatch + ", " + attemptNumber + " attempts, "
        + callable + ")";
  }

  private V doRetry() throws RetryHelperException {
    stopwatch.start();
    while (true) {
      attemptNumber++;
      Exception exception;
      try {
        V value = callable.call();
        if (attemptNumber > 1) {
          log.info(this + ": attempt #" + attemptNumber + " succeeded");
        }
        return value;
      } catch (Exception e) {
        if (!exceptionHandler.shouldRetry(e)) {
          if (e instanceof InterruptedException ||
              e instanceof InterruptedIOException ||
              e instanceof ClosedByInterruptException) {
            RetryInterruptedException.propagate();
          }
          throw new NonRetriableException(e);
        }
        exception = e;
      }
      if (attemptNumber >= params.getRetryMaxAttempts() || (
          attemptNumber >= params.getRetryMinAttempts()
          && stopwatch.elapsed(MILLISECONDS) >= params.getTotalRetryPeriodMillis())) {
        throw new RetriesExhaustedException(this + ": Too many failures, giving up", exception);
      }
      long sleepDurationMillis = getSleepDuration(params, attemptNumber);
      log.info(this + ": Attempt #" + attemptNumber + " failed [" + exception + "], sleeping for "
          + sleepDurationMillis + " ms");
      try {
        Thread.sleep(sleepDurationMillis);
      } catch (InterruptedException e) {
        RetryInterruptedException.propagate();
      }
    }
  }

  @VisibleForTesting
  static long getSleepDuration(RetryParams retryParams, int attemptsSoFar) {
    long initialDelay = retryParams.getInitialRetryDelayMillis();
    double backoff = retryParams.getRetryDelayBackoffFactor();
    long maxDelay = retryParams.getMaxRetryDelayMillis();
    long retryDelay = getExponentialValue(initialDelay, backoff, maxDelay, attemptsSoFar);
    return (long) ((Math.random() / 2.0 + .75) * retryDelay);
  }

  public static <V> V runWithRetries(Callable<V> callable) throws RetryHelperException {
    return runWithRetries(callable, RetryParams.getDefaultInstance(),
        ExceptionHandler.getDefaultInstance());
  }

  public static <V> V runWithRetries(Callable<V> callable, RetryParams params,
      ExceptionHandler exceptionHandler) throws RetryHelperException {
    return runWithRetries(callable, params, exceptionHandler, Stopwatch.createUnstarted());
  }

  @VisibleForTesting
  static <V> V runWithRetries(Callable<V> callable, RetryParams params,
      ExceptionHandler exceptionHandler, Stopwatch stopwatch) throws RetryHelperException {
    RetryHelper<V> retryHelper = new RetryHelper<>(callable, params, exceptionHandler, stopwatch);
    Context previousContext = getContext();
    setContext(new Context(retryHelper));
    try {
      return retryHelper.doRetry();
    } finally {
      setContext(previousContext);
    }
  }
}
