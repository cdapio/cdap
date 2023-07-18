/*
 * Copyright 2015 Cask Data, Inc.
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
package io.cdap.cdap.api.metrics;

import java.util.Arrays;

/**
 * Carries the "raw" emitted metric data point: metric name, type, and value
 */
public class MetricValue {

  private final String name;
  private final MetricType type;
  private final long value;

  // following fields to support distribution aka histogram/event metrics
  /**
   * Exponential rate at which bucket boundaries grow. If X is the growth facotr, bucket boundaries
   * will 0-X^0 (i.e. 1), X^0-X, X-X^2, X^2-X^3, X^3-X^4.
   */
  public static final int GROWTH_FACTOR = 2;

  /**
   * Total buckets = NUM_FINITE_BUCKETS +2 which will be 64. A long number maybe used as a mask to
   * indicate which buckets have non zero bucket counts
   */
  public static final int NUM_FINITE_BUCKETS = Long.SIZE - 2;

  private final long[] bucketCounts;

  private final long bucketMask;

  private final double sum;

  public MetricValue(String name, MetricType type, long value) {
    if (!(type == MetricType.GAUGE || type == MetricType.COUNTER)) {
      throw new IllegalArgumentException("long value allowed only for GAUGE or COUNTER metrics");
    }
    this.name = name;
    this.type = type;
    this.value = value;
    sum = 0;
    bucketMask = 0;
    bucketCounts = null;
  }

  public MetricValue(String name, long[] bucketCounts, long bucketMask,
      double sum) {
    if (bucketCounts == null) {
      throw new IllegalArgumentException("bucketCounts should not be null");
    }
    this.name = name;
    this.bucketMask = bucketMask;
    this.sum = sum;
    this.type = MetricType.DISTRIBUTION;
    this.bucketCounts = bucketCounts;
    value = 0;
  }

  public String getName() {
    return name;
  }

  public MetricType getType() {
    return type;
  }

  public long getValue() {
    if (type != MetricType.COUNTER && type != MetricType.GAUGE) {
      throw new IllegalStateException("getValue allowed for only COUNTER or GAUGE metrics");
    }
    return value;
  }

  @Override
  public String toString() {
    if (type != MetricType.DISTRIBUTION) {
      return "MetricValue{"
          + "name='" + name + '\''
          + ", type=" + type
          + ", value=" + value
          + '}';
    } else {
      return "MetricValue{"
          + "name='" + name + '\''
          + ", type=" + type
          + ", sum=" + sum
          + ", bucketMask=" + bucketMask
          + ", bucketCounts=" + Arrays.toString(bucketCounts)
          + '}';
    }
  }

  /**
   * bucketCounts stores counts of buckets with non zero values.
   */
  public long[] getBucketCounts() {
    if (type != MetricType.DISTRIBUTION) {
      throw new IllegalStateException("getBucketCounts allowed only for Distribution Metric");
    }
    return bucketCounts;
  }

  /**
   * @return Counts for all buckets including buckets with zero counts. Helper function to publish
   *     the distribution metric to other systems such as Cloud Monitoring
   */
  public long[] getAllBucketCounts() {
    if (type != MetricType.DISTRIBUTION) {
      throw new IllegalStateException("getAllBucketCounts allowed only for Distribution Metric");
    }

    long[] allBucketCounts = new long[NUM_FINITE_BUCKETS + 2];
    long mask = 1;
    int index = 0;
    if (bucketCounts != null) {
      for (int i = 0; i < allBucketCounts.length; i++) {
        if ((bucketMask & mask) >= 1) {
          allBucketCounts[i] = bucketCounts[index];
          index++;
        }
        mask = mask << 1;
      }
    }
    return allBucketCounts;
  }

  /**
   * Most of the buckets have zero counts. For efficiency, bucketCounts only stores non zero bucket
   * count values. The bucketMask stores which buckets have non-zero bucketCounts.
   *
   * The right most bit i.e. least significant bit (LSB) stands for bucket 0 with range -infinity to
   * 0.
   *
   * Note that, except the LSB, most of the range is for positive values.
   *
   * @return bucketmask
   */
  public long getBucketMask() {
    if (type != MetricType.DISTRIBUTION) {
      throw new IllegalStateException("getBucketMask allowed only for Distribution Metric");
    }
    return bucketMask;
  }

  /**
   * Sum of all the values. If count is zero then this field must be zero.
   */
  public double getSum() {
    if (type != MetricType.DISTRIBUTION) {
      throw new IllegalStateException("getSum allowed only for Distribution Metric");
    }
    return sum;
  }
}
