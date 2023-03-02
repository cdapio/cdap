/*
 * Copyright © 2022 Cask Data, Inc.
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
package io.cdap.cdap.metrics.collect;

import io.cdap.cdap.api.metrics.MetricType;
import io.cdap.cdap.api.metrics.MetricValue;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.LongAdder;
import org.junit.Assert;
import org.junit.Test;

public class DistributionTest {
  private static final String METRIC_NAME = "test_distribution";
  private static final double TINY_DELTA = 0.0001;

  @Test
  public void testEmptyDistribution() {
    Distribution distribution = new Distribution();
    verifyDistribution(distribution, 0, 0, new long[]{});
  }


  private void verifyDistribution(Distribution distribution, long expectedMask, double expectedSum,
        long[] bucketCounts) {
    MetricValue metricValue = distribution.getMetricValue(METRIC_NAME);
    Assert.assertEquals(METRIC_NAME, metricValue.getName());
    Assert.assertEquals(MetricType.DISTRIBUTION, metricValue.getType());
    Assert.assertEquals(expectedMask, metricValue.getBucketMask());
    Assert.assertEquals(expectedSum, metricValue.getSum(), TINY_DELTA);
    Assert.assertArrayEquals(bucketCounts, metricValue.getBucketCounts());
}

  @Test
  public void testNegativeValues() {
    Distribution distribution = new Distribution();
    distribution.add(Long.MIN_VALUE);
    verifyDistribution(distribution, 1, Long.MIN_VALUE, new long[]{1});
  }

  @Test
  public void testFullRangeDistribution() {
    Distribution distribution = new Distribution();
    distribution.add(0);
    distribution.add(1);
    // 2 multiple times
    distribution.add(2);
    distribution.add(2);
    distribution.add(16);
    distribution.add(100);
    // test with odd number
    distribution.add(101);
    long mask = 2 /* value 0 falls in bucket 0-1 which is the 2nd bucket*/
        + 4 /* value 1 falls in bucket 1-2 which is the 3rd bucket*/
        + 8 /* value 2 falls in bucket 2-4 which is the 4th bucket */
        + 64 /* value 16 falls in bucket 16-32 which is the 7th bucket */
        + 256 /*value 100,101 falls in bucket 64-128 which is the 9th bucket */;
    double expSum = 1 + 2 + 2 + 16 + 100 + 101;

    verifyDistribution(distribution, mask, expSum, new long[]{1, 1, 2, 1, 2});
  }

  @Test
  public void testMaxValue() {
    Distribution distribution = new Distribution();
    distribution.add(Long.MAX_VALUE);
    verifyDistribution(distribution, 1L << 63, Long.MAX_VALUE, new long[]{1});
  }

  // TODO mark as slow test
  @Test
  public void testAggregatedEmitterConcurrency() throws InterruptedException {
    AggregatedMetricsEmitter emitter = new AggregatedMetricsEmitter("ignore");
    AtomicBoolean end = new AtomicBoolean(false);
    LongAdder totalAdds = new LongAdder();
    LongAdder totalEmitCount = new LongAdder();

    Thread emittingThread = new Thread(() -> {
      for (int i = 0; i < 10; i++) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          // ignore
        }
        MetricValue metricValue = emitter.emit();
        totalEmitCount.add(Arrays.stream(metricValue.getBucketCounts()).sum());
      }
      end.set(true);
    });

    Runnable runnable = () -> {
      while (!end.get()) {
        emitter.event(1);
        totalAdds.add(1);
      }
    };

    Thread addingThread1 = new Thread(runnable);
    Thread addingThread2 = new Thread(runnable);
    addingThread1.start();
    addingThread2.start();
    emittingThread.start();

    emittingThread.join();
    addingThread1.join();
    addingThread2.join();

    // final emit
    MetricValue metricValue = emitter.emit();
    totalEmitCount.add(Arrays.stream(metricValue.getBucketCounts()).sum());

    Assert.assertEquals(totalAdds.longValue(), totalEmitCount.longValue());
    System.out.println(String.format("totalAdds: %d", totalAdds.longValue()));
  }
}
