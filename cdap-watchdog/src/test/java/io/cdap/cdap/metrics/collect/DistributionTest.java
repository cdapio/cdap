/*
 * Copyright © 2014-2017 Cask Data, Inc.
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
import org.junit.Assert;
import org.junit.Test;

public class DistributionTest {
    private static final String METRIC_NAME = "test_distribution";
    private static final double TINY_DELTA = 0.0001;

    @Test
    public void testEmptyDistribution() {
        Distribution distribution = new Distribution();
        verifyDistribution(distribution, 0, 0, 0.0, new long[]{});
    }

    private void verifyDistribution(Distribution distribution, long expectedMask, double expectedMean,
                                    double expectedSumOfSquaredDeviation, long[] bucketCounts) {
        MetricValue metricValue = distribution.getMetricValue(METRIC_NAME);
        Assert.assertEquals(METRIC_NAME, metricValue.getName());
        Assert.assertEquals(MetricType.DISTRIBUTION, metricValue.getType());
        Assert.assertEquals(expectedMask, metricValue.getBucketMask());
        Assert.assertEquals(expectedMean, metricValue.getMean(), TINY_DELTA);
        Assert.assertEquals(expectedSumOfSquaredDeviation, metricValue.getSumOfSquaredDeviation(), TINY_DELTA);
        Assert.assertArrayEquals(bucketCounts, metricValue.getBucketCounts());
    }

    @Test
    public void testNegativeValues() {
        Distribution distribution = new Distribution();
        distribution.add(Long.MIN_VALUE);
        verifyDistribution(distribution, 1, Long.MIN_VALUE, 0.0, new long[]{1});

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
        long mask = 2 /* value 0 falls in bucket 0-1 which is the 2nd bucket*/ +
                4 /* value 1 falls in bucket 1-2 which is the 3rd bucket*/ +
                8 /* value 2 falls in bucket 2-4 which is the 4th bucket */ +
                64 /* value 16 falls in bucket 16-32 which is the 7th bucket */ +
                256 /*value 100,101 falls in bucket 64-128 which is the 9th bucket */;
        double expMean = (1 + 2 + 2 + 16 + 100 + 101) / 7.0;
        double expSumOfSquaredDeviation = expMean * expMean +
                (expMean - 1) * (expMean - 1) +
                (expMean - 2) * (expMean - 2) +
                (expMean - 2) * (expMean - 2) +
                (expMean - 16) * (expMean - 16) +
                (expMean - 100) * (expMean - 100) +
                (expMean - 101) * (expMean - 101);

        verifyDistribution(distribution, mask, expMean, expSumOfSquaredDeviation,
                new long[]{1 , 1 , 2 , 1 , 2});
    }

    @Test
    public void testMaxValue() {
        Distribution distribution = new Distribution();
        distribution.add(Long.MAX_VALUE);
        verifyDistribution(distribution, 1 << 63, Long.MAX_VALUE, 0.0, new long[]{1});
    }
}
