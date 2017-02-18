/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.common.logging;

import co.cask.cdap.common.utils.TimeProvider;
import com.google.common.base.Supplier;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LocationAwareLogger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Unit test for different {@link LogSampler} as created through {@link LogSamplers}.
 */
public class LogSamplerTest {

  private static final Logger LOG = LoggerFactory.getLogger(LogSamplerTest.class);

  @Test
  public void testOnceEvery() {
    LogSampler sampler = LogSamplers.onceEvery(10);
    List<Integer> accepted = new ArrayList<>();

    for (int i = 0; i < 100; i++) {
      if (sampler.accept("", 0)) {
        accepted.add(i);
      }
    }
    Assert.assertEquals(Arrays.asList(0, 10, 20, 30, 40, 50, 60, 70, 80, 90), accepted);
  }

  @Test
  public void testExponentialLimit() {
    LogSampler sampler = LogSamplers.exponentialLimit(1, 1000, 2.0d);
    List<Integer> accepted = new ArrayList<>();

    for (int i = 0; i < 3000; i++) {
      if (sampler.accept("", 0)) {
        accepted.add(i);
      }
    }

    Assert.assertEquals(Arrays.asList(0, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1000, 2000), accepted);
  }

  @Test
  public void testConcurrentLimitRate() throws InterruptedException {
    final LogSampler sampler = LogSamplers.limitRate(50, new TimeProvider.IncrementalTimeProvider());
    final AtomicInteger acceptedCount = new AtomicInteger();

    int threadCount = 5;
    final CyclicBarrier barrier = new CyclicBarrier(threadCount);
    final CountDownLatch completeLatch = new CountDownLatch(threadCount);
    for (int i = 0; i < 5; i++) {
      final int threadId = i;
      new Thread() {
        @Override
        public void run() {
          try {
            barrier.await();
            for (int i = 0; i < 100; i++) {
              if (sampler.accept("", 0)) {
                acceptedCount.incrementAndGet();
              }
            }
          } catch (Exception e) {
            LOG.error("Thread {} throws exception", threadId, e);
          } finally {
            completeLatch.countDown();
          }
        }
      }.start();
    }

    Assert.assertTrue(completeLatch.await(10, SECONDS));

    // The limit rate sampler should still maintain the correct rate under concurrent calls
    // We are emitting 500 messages, with accept rate of once per 50 milliseconds. The time provider we used
    // for testing always generates unique timestamp, hence we should expect exactly 10 messages being accepted.
    Assert.assertEquals(10, acceptedCount.get());
  }

  @Test
  public void testLimitRate() {
    // Instead of sleeping, use the incremental time provider to generate monotonic increase timestamps
    LogSampler sampler = LogSamplers.limitRate(200, new TimeProvider.IncrementalTimeProvider());
    List<Integer> accepted = new ArrayList<>();

    for (int i = 0; i < 1000; i++) {
      if (sampler.accept("", 0)) {
        accepted.add(i);
      }
    }

    Assert.assertEquals(Arrays.asList(0, 200, 400, 600, 800), accepted);
  }

  @Test
  public void testOnMessages() {
    LogSampler sampler = LogSamplers.onMessages(LogSamplers.onceEvery(3), "test1", "test5", "test8");
    List<Integer> accepted = new ArrayList<>();

    for (int i = 0; i < 10; i++) {
      if (sampler.accept("test" + i, 0)) {
        accepted.add(i);
      }
    }

    // The sampler only applies on messages test1, test5, and test8 with limit count of 3.
    Assert.assertEquals(Arrays.asList(0, 1, 2, 3, 4, 6, 7, 9), accepted);
  }

  @Test
  public void testOnPatternFull() {
    LogSampler sampler = LogSamplers.onPattern(LogSamplers.onceEvery(4), Pattern.compile("test1\\d+"), true);
    List<Integer> accepted = new ArrayList<>();

    for (int i = 5; i < 20; i++) {
      if (sampler.accept("test" + i, 0)) {
        accepted.add(i);
      }
    }

    // The sampler only applies on messages test10 - test19 with limit count of 4.
    Assert.assertEquals(Arrays.asList(5, 6, 7, 8, 9, 10, 14, 18), accepted);
  }

  @Test
  public void testOnPatternPartial() {
    LogSampler sampler = LogSamplers.onPattern(LogSamplers.onceEvery(4), Pattern.compile("st1\\d+"), false);
    List<Integer> accepted = new ArrayList<>();

    for (int i = 5; i < 20; i++) {
      if (sampler.accept("test" + i, 0)) {
        accepted.add(i);
      }
    }

    // The sampler only applies on messages test10 - test19 with limit count of 4.
    Assert.assertEquals(Arrays.asList(5, 6, 7, 8, 9, 10, 14, 18), accepted);
  }

  @Test
  public void testOnLogLevel() {
    List<Integer> accepted = new ArrayList<>();

    // Samples on TRACE or lower
    runAcceptOnLogLevels(LogSamplers.onTrace(LogSamplers.onceEvery(10)), accepted);
    Assert.assertEquals(Arrays.asList(
      0,                              // TRACE
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,   // DEBUG
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,   // INFO
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,   // WARN
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9    // ERROR
    ), accepted);

    accepted.clear();

    // Samples on DEBUG or lower
    runAcceptOnLogLevels(LogSamplers.onDebug(LogSamplers.onceEvery(10)), accepted);
    Assert.assertEquals(Arrays.asList(
      0,                              // TRACE
      0,                              // DEBUG
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,   // INFO
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,   // WARN
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9    // ERROR
    ), accepted);

    accepted.clear();

    // Samples on INFO or lower
    runAcceptOnLogLevels(LogSamplers.onInfo(LogSamplers.onceEvery(10)), accepted);
    Assert.assertEquals(Arrays.asList(
      0,                              // TRACE
      0,                              // DEBUG
      0,                              // INFO
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9,   // WARN
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9    // ERROR
    ), accepted);


    accepted.clear();

    // Samples on WARN or lower
    runAcceptOnLogLevels(LogSamplers.onWarn(LogSamplers.onceEvery(10)), accepted);
    Assert.assertEquals(Arrays.asList(
      0,                              // TRACE
      0,                              // DEBUG
      0,                              // INFO
      0,                              // WARN
      0, 1, 2, 3, 4, 5, 6, 7, 8, 9    // ERROR
    ), accepted);


    accepted.clear();

    // Samples on ERROR or lower
    runAcceptOnLogLevels(LogSamplers.onError(LogSamplers.onceEvery(10)), accepted);
    Assert.assertEquals(Arrays.asList(
      0,                              // TRACE
      0,                              // DEBUG
      0,                              // INFO
      0,                              // WARN
      0                               // ERROR
    ), accepted);
  }

  @Test
  public void testAny() {
    // A sampler that accept once every 500 count, but also accept if time passed 200 milliseconds
    LogSampler sampler = LogSamplers.any(LogSamplers.onceEvery(500),
                                         LogSamplers.limitRate(200, new TimeProvider.IncrementalTimeProvider()));
    List<Integer> accepted = new ArrayList<>();

    for (int i = 0; i < 2000; i++) {
      if (sampler.accept("", 0)) {
        accepted.add(i);
      }
    }

    Assert.assertEquals(Arrays.asList(0, 200, 400, 500, 600, 800, 1000, 1200, 1400, 1500, 1600, 1800), accepted);
  }

  @Test
  public void testAll() {
    LogSampler sampler = LogSamplers.all(LogSamplers.onceEvery(500),
                                         LogSamplers.limitRate(200, new TimeProvider.IncrementalTimeProvider()));
    List<Integer> accepted = new ArrayList<>();

    for (int i = 0; i < 2000; i++) {
      if (sampler.accept("", 0)) {
        accepted.add(i);
      }
    }

    Assert.assertEquals(Arrays.asList(0, 1000), accepted);
  }

  @Test
  public void testPerMessage() {
    LogSampler sampler = LogSamplers.perMessage(new Supplier<LogSampler>() {
      @Override
      public LogSampler get() {
        return LogSamplers.onceEvery(3);
      }
    });

    List<Integer> accepted = new ArrayList<>();
    // With same message, should be using the same sampler
    for (int i = 0; i < 10; i++) {
      if (sampler.accept("test", 0)) {
        accepted.add(i);
      }
    }

    Assert.assertEquals(Arrays.asList(0, 3, 6, 9), accepted);

    // With a different message, should be using different sampler, hence the counting should be reset
    accepted.clear();
    for (int i = 0; i < 10; i++) {
      if (sampler.accept("test2", 0)) {
        accepted.add(i);
      }
    }
    Assert.assertEquals(Arrays.asList(0, 3, 6, 9), accepted);
  }

  private void runAcceptOnLogLevels(LogSampler sampler, List<Integer> accepted) {
    List<Integer> logLevels = Arrays.asList(LocationAwareLogger.TRACE_INT,
                                            LocationAwareLogger.DEBUG_INT,
                                            LocationAwareLogger.INFO_INT,
                                            LocationAwareLogger.WARN_INT,
                                            LocationAwareLogger.ERROR_INT);
    for (int logLevel : logLevels) {
      for (int i = 0; i < 10; i++) {
        if (sampler.accept("", logLevel)) {
          accepted.add(i);
        }
      }
    }
  }
}
