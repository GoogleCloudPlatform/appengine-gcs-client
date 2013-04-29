package com.google.appengine.tools.cloudstorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.google.appengine.tools.cloudstorage.RetryHelper.Body;
import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Tests for Retry helper.
 *
 */
@RunWith(JUnit4.class)
public class RetryHelperTest {

  @Test
  public void testTriesAtLeastMinTimes() throws IOException {
    RetryParams parms = new RetryParams();
    parms.setInitialRetryDelayMillis(0);
    parms.setRetryPeriodMillis(20000);
    parms.setRetryMinAttempts(5);
    parms.setRetryMaxAttempts(10);
    final int timesToFail = 7;
    int attempted = RetryHelper.runWithRetries(new Body<Integer>() {
      int timesCalled = 0;

      @Override
      public Integer run() throws IOException {
        timesCalled++;
        if (timesCalled <= timesToFail) {
          throw new IOException();
        } else {
          return timesCalled;
        }
      }
    }, parms);
    assertEquals(timesToFail + 1, attempted);
  }

  private class FakeTicker extends Ticker {
    private final AtomicLong nanos = new AtomicLong();

    /** Advances the ticker value by {@code time} in {@code timeUnit}. */
    FakeTicker advance(long time, TimeUnit timeUnit) {
      return advance(timeUnit.toNanos(time));
    }

    /** Advances the ticker value by {@code nanoseconds}. */
    FakeTicker advance(long nanoseconds) {
      nanos.addAndGet(nanoseconds);
      return this;
    }

    @Override
    public long read() {
      return nanos.get();
    }
  }

  @Test
  public void testTriesNoMoreThanMaxTimes() throws IOException {
    final FakeTicker ticker = new FakeTicker();
    Stopwatch stopwatch = new Stopwatch(ticker);
    RetryParams parms = new RetryParams();
    parms.setInitialRetryDelayMillis(0);
    parms.setRetryPeriodMillis(500);
    parms.setRetryMinAttempts(5);
    parms.setRetryMaxAttempts(10);
    final int timesToFail = 20;
    final int sleepOnAttempt = 8;
    final AtomicInteger timesCalled = new AtomicInteger(0);
    try {
      RetryHelper.runWithRetries(new Body<Integer>() {
        @Override
        public Integer run() throws IOException {
          timesCalled.incrementAndGet();
          if (timesCalled.get() == sleepOnAttempt) {
            ticker.advance(1000, TimeUnit.MILLISECONDS);
          }
          if (timesCalled.get() <= timesToFail) {
            throw new IOException();
          } else {
            return timesCalled.get();
          }
        }
      }, parms, stopwatch);
      fail();
    } catch (RetriesExhaustedException e) {
      assertEquals(sleepOnAttempt, timesCalled.get());
    }
  }

  @Test
  public void testBackoffIsExponential() {
    RetryParams parms = new RetryParams();
    parms.setInitialRetryDelayMillis(10);
    parms.setMaxRetryDelayMillis(10000000);
    parms.setRetryPeriodMillis(1000);
    parms.setRetryMinAttempts(0);
    parms.setRetryMaxAttempts(100);
    final int timesToFail = 200;
    long sleepDuration = RetryHelper.getSleepDuration(parms, 1);
    assertTrue("" + sleepDuration, sleepDuration < 10 && sleepDuration >= 5);
    sleepDuration = RetryHelper.getSleepDuration(parms, 2);
    assertTrue("" + sleepDuration, sleepDuration < 20 && sleepDuration >= 10);
    sleepDuration = RetryHelper.getSleepDuration(parms, 3);
    assertTrue("" + sleepDuration, sleepDuration < 40 && sleepDuration >= 20);
    sleepDuration = RetryHelper.getSleepDuration(parms, 4);
    assertTrue("" + sleepDuration, sleepDuration < 80 && sleepDuration >= 40);
    sleepDuration = RetryHelper.getSleepDuration(parms, 5);
    assertTrue("" + sleepDuration, sleepDuration < 160 && sleepDuration >= 80);
    sleepDuration = RetryHelper.getSleepDuration(parms, 6);
    assertTrue("" + sleepDuration, sleepDuration < 320 && sleepDuration >= 160);
    sleepDuration = RetryHelper.getSleepDuration(parms, 7);
    assertTrue("" + sleepDuration, sleepDuration < 640 && sleepDuration >= 320);
    sleepDuration = RetryHelper.getSleepDuration(parms, 8);
    assertTrue("" + sleepDuration, sleepDuration < 1280 && sleepDuration >= 640);
    sleepDuration = RetryHelper.getSleepDuration(parms, 9);
    assertTrue("" + sleepDuration, sleepDuration < 2560 && sleepDuration >= 1280);
    sleepDuration = RetryHelper.getSleepDuration(parms, 10);
    assertTrue("" + sleepDuration, sleepDuration < 5120 && sleepDuration >= 2560);
    sleepDuration = RetryHelper.getSleepDuration(parms, 11);
    assertTrue("" + sleepDuration, sleepDuration < 10240 && sleepDuration >= 5120);
    sleepDuration = RetryHelper.getSleepDuration(parms, 12);
    assertTrue("" + sleepDuration, sleepDuration < 20480 && sleepDuration >= 10240);
  }

}
