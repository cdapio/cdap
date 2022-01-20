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

  private String name;
  private MetricType type;
  private long value;

  // following fields to support distribution aka histogram/event metrics
  /**
   * Exponential rate at which bucket boundaries grow.
   * Based on https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TypedValue#Exponential
   */
  public static final int GROWTH_FACTOR = 2;

  /**
   * Total buckets = NUM_FINITE_BUCKETS +2 which will be 64. A long number
   * maybe used as a mask to indicate which buckets have non zero bucket counts
   */

  public static final int NUM_FINITE_BUCKETS = 62;

  /**
   * Based on https://cloud.google.com/monitoring/api/ref_v3/rest/v3/TypedValue#Distribution
   *
   * bucketCounts stores counts of buckets with non zero values.
   */
  private long[] bucketCounts;

  /**
   * Most of the buckets have zero counts. For efficiency, bucketCounts only
   * stores non zero bucket count values. The bucketMask stores which
   * buckets have non-zero bucketCounts.
   *
   * The right most bit i.e. least significant bit (LSB) stands for bucket 0.
   */
  private long bucketMask;

  /**
   * If count is zero then this field must be zero. Mean can be easily calculated from sum and count.
   */
  private double mean;

  /**
   * The sum of squared deviations from the mean of the values in the population.
   */
  private double sumOfSquaredDeviation;

  public MetricValue (String name, MetricType type, long value) {
    if (!(type == MetricType.GAUGE || type == MetricType.COUNTER)) {
      throw new IllegalArgumentException("long value allowed only for GAUGE or COUNTER metrics");
    }
    this.name = name;
    this.type = type;
    this.value = value;
  }

  public MetricValue(String name, long[] bucketCounts, long bucketMask,
                     double mean, double sumOfSquaredDeviation) {
    this.name = name;
    this.bucketMask = bucketMask;
    this.mean = mean;
    this.sumOfSquaredDeviation = sumOfSquaredDeviation;
    this.type = MetricType.DISTRIBUTION;
    this.bucketCounts = bucketCounts;
  }

  public String getName() {
    return name;
  }

  public MetricType getType() {
    return type;
  }

  public long getValue() {
    return value;
  }

  @Override
  public String toString() {
    if (type != MetricType.DISTRIBUTION) {
      return "MetricValue{" +
              "name='" + name + '\'' +
              ", type=" + type +
              ", value=" + value +
              '}';
    } else {
      return "MetricValue{" +
              "name='" + name + '\'' +
              ", type=" + type +
              ", mean=" + mean +
              ", sumOfSquaredDeviation=" + sumOfSquaredDeviation +
              ", bucketMask=" + bucketMask +
              ", bucketCounts=" + Arrays.toString(bucketCounts) +
              '}';
    }
  }

  public long[] getBucketCounts() {
    return bucketCounts;
  }

  /**
   * @return Counts for all buckets including buckets with zero counts.
   * Helper function to publish the distribution metric to other systems such as Cloud Monitoring
   */
  public long[] getAllBucketCounts() {
    long[] allBucketCounts = new long[NUM_FINITE_BUCKETS + 2];
    long mask = 1;
    int index = 0;
    for (int i = 0; i < allBucketCounts.length; i++) {
      if ((bucketMask & mask) >= 1) {
        allBucketCounts[i] = bucketCounts[index];
        index++;
      }
      mask = mask << 1;
    }
    return allBucketCounts;
  }

  public long getBucketMask() {
    return bucketMask;
  }

  public double getMean() {
    return mean;
  }

  public double getSumOfSquaredDeviation() {
    return sumOfSquaredDeviation;
  }
}
