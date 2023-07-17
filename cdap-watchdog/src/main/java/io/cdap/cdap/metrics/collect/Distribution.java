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

import io.cdap.cdap.api.metrics.MetricValue;
import java.util.Arrays;

/**
 * Class is not thread-safe. It is the caller's responsibility to ensure thread safety.
 */
public class Distribution {
  /**
   * Trying to minimize memory usage for the buckets. Using a long[64 (i.e. ROWS*COLS)] will always store 64 numbers,
   * most of them being 0. A HashMap is an alternative, but boxing overhead is high (roughly 10X) for
   * each long.
   *
   * A third alternative used is to increase array size till the highest bucket. But this involves
   * repeatedly copying the array.
   *
   * The current approach strikes a balance by using an array of arrays. Ex. bucket 52 is stored in
   * the 6th row, 5th column (6*8(i.e. ROWS) + 5th column). Initially all rows are null. The moment a bucket increases
   * by 1, a long[8 (i.e. COLS)] is created for that row. When bucket 52 becomes non-zero, the 6th row is initialized
   * to long[8 (i.e. COLS)] and the 5th column is increased to 1. If bucket 53 needs to be incremented, the row
   * already exists. So the 6th column is incremented.
   *
   * In the worst case, this can take storage for 64 longs + 8(ROWS) rows references. But in the normal case,
   * the range of values for a metric will probably within a 256X range i.e. max value <= min * 256. So at most
   * two rows will have to be initialized, resulting in 8(COLS)*2 long number storage + 8(ROWS) object references.
   */

  /**
   * ROWS is the number of rows in bucketCounts and COLS is the number of columns in bucketCounts.
   * Note that ROWS*COLS MUST be less than or equal to 64 (Long.SIZE), since we are using a long
   * bucketMask.
   */
  private static final int ROWS = 8;
  private static final int COLS = 8;

  private final long[][] bucketCounts = new long[ROWS][];

  /**
   * If count is zero then this field must be zero. Mean can be easily calculated from sum and
   * count.
   */
  private double sum;

  private long count;

  private int numBuckets;

  public void add(long value) {
    int bucket = findBucketNumber(value);
    count += 1;
    sum += value;

    int i = getRow(bucket);
    int j = getCol(bucket);
    if (bucketCounts[i] == null) {
      bucketCounts[i] = new long[COLS];
    }
    if (bucketCounts[i][j] == 0) {
      numBuckets++;
    }
    bucketCounts[i][j] += 1;
  }

  private int findBucketNumber(long value) {
    // negative values map to bucket 0
    if (value < 0) {
      return 0;
    }
    // value: 0 maps to bucket 1
    if (value == 0) {
      return 1;
    }
    // Long.numberOfTrailingZeros(Long.highestOneBit(value)) returns 0 for value: 1. Hence +2
    return Math.min(Long.numberOfTrailingZeros(Long.highestOneBit(value)) + 2, Long.SIZE - 1);
  }

  private int getRow(int bucket) {
    // Hotspot will optimize this division
    // https://stackoverflow.com/questions/1514949/quick-java-optimization-question
    return bucket / COLS;
  }

  private int getCol(int bucket) {
    // Hotspot will optimize this modulo
    // https://stackoverflow.com/questions/29844345/how-is-the-modulo-operator-implemented-in-the-hotspot-jvm
    return bucket % COLS;
  }

  public MetricValue getMetricValue(String metricName) {
    long bucketMask = 0;
    long[] bucketCountsArr = new long[numBuckets];
    int bucketCountIndex = 0;
    for (int row = 0; row < ROWS; row++) {
      if (bucketCounts[row] == null) {
        continue;
      }
      for (int col = 0; col < COLS; col++) {
        if (bucketCounts[row][col] > 0) {
          bucketMask |= 1L << row * COLS + col;
          bucketCountsArr[bucketCountIndex] = bucketCounts[row][col];
          bucketCountIndex++;
        }
      }
    }
    return new MetricValue(metricName, bucketCountsArr, bucketMask, sum);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append(String.format("Distribution {count: %d, numBuckets: %d, sum: %f ",
        count, numBuckets, sum));
    for (int index = 0; index < ROWS; index++) {
      if (bucketCounts[index] != null) {
        sb.append(String.format("Row %d: %s ", index, Arrays.toString(bucketCounts[index])));
      } else {
        sb.append(String.format("%d: null ", index));
      }
    }
    sb.append("}");
    return sb.toString();
  }
}
