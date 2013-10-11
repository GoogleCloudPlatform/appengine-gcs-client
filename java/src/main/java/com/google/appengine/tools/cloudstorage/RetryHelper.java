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
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.apphosting.api.AppEngineInternal;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;

import java.io.InterruptedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility class for retrying operations. For more details about the parameters, see
 * {@link RetryParams}
 *
 * If the request is never successful, a {@link RetriesExhaustedException} will be thrown.
 *
 * @author ohler@google.com (Christian Ohler)
 *
 * @param <V> return value of the closure that is being run with retries
 */
@AppEngineInternal
public class RetryHelper<V>  {

  private static final Logger log = Logger.getLogger(RetryHelper.class.getName());

  private final Stopwatch stopwatch;
  private final Callable<V> callable;
  private final RetryParams params;
  private final ExceptionHandler exceptionHandler;
  private int attemptsSoFar;

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
    return getClass().getSimpleName() + "(" + stopwatch + ", " + attemptsSoFar + " attempts, "
        + callable + ")";
  }

  private V doRetry() throws RetryHelperException {
    stopwatch.start();
    while (true) {
      attemptsSoFar++;
      Exception exception;
      try {
        V value = callable.call();
        if (attemptsSoFar > 1) {
          log.info(this + ": attempt #" + (attemptsSoFar) + " succeeded");
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
      long sleepDurationMillis = getSleepDuration(params, attemptsSoFar);
      log.log(Level.WARNING, this + ": Attempt " + attemptsSoFar + " failed, sleeping for "
          + sleepDurationMillis + " ms", exception);

      if (attemptsSoFar >= params.getRetryMaxAttempts() || (
          attemptsSoFar >= params.getRetryMinAttempts()
          && stopwatch.elapsed(MILLISECONDS) >= params.getTotalRetryPeriodMillis())) {
        throw new RetriesExhaustedException(this + ": Too many failures, giving up", exception);
      }
      try {
        Thread.sleep(sleepDurationMillis);
      } catch (InterruptedException e) {
        RetryInterruptedException.propagate();
      }
    }
  }

  @VisibleForTesting
  static long getSleepDuration(RetryParams retryParams, int attemptsSoFar) {
    return (long) ((Math.random() / 2.0 + .5) * (Math.min(
        retryParams.getMaxRetryDelayMillis(),
        Math.pow(retryParams.getRetryDelayBackoffFactor(), attemptsSoFar - 1)
        * retryParams.getInitialRetryDelayMillis())));
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
    return new RetryHelper<V>(callable, params, exceptionHandler, stopwatch).doRetry();
  }
}
