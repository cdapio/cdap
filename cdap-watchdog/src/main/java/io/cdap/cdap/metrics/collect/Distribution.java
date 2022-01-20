/*
 * Copyright © 2014 Cask Data, Inc.
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

public class Distribution {
    /**
     * Trying to minimize memory usage for the buckets. Using a long[64] will always store 64 numbers,
     * most of them being 0. A HashMap is an alternative, but boxing overhed is high (roughly 10X) for
     * each long.
     *
     * A third alternative used is to increase array size till the highest bucket. But this involves
     * repeatedly copying the array.
     *
     * The current approach strikes a balance by using an array of arrays. Ex. bucket 52 is stored in
     * the 6th row, 5th column (6*8 + 5th column). Initially all rows are null. The moment a bucket increases
     * by 1, a long[8] is created for that row. When bucket 52 becomes non-zero, the 6th row is initialized to
     * long[8] and the 5th column is increased to 1. If bucket 53 needs to be incremented, the row already
     * exists. So the 6th column is incremented.
     *
     * In the worst case, this can take storage for 64 longs + 8 row references. But in the normal case, the range
     * of values for a metric will probably within a 256X range i.e. max value <= min * 256. So at most two rows
     * will have to be initialized, resulting in 8*2 long number storage + 8 object references.
     */
    private long[][] bucketCounts = new long[8][];

    /**
     * If count is zero then this field must be zero. Mean can be easily calculated from sum and count.
     */
    private double mean;

    private long count;

    private int numBuckets;

    /**
     * The sum of squared deviations from the mean of the values in the population.
     */
    private double sumOfSquaredDeviation;

    public void add(long value) {
        int index = findBucketNumber(value);
        synchronized (this) {
            // online calculation of mean and sumOfSquaredDeviation
            count += 1;
            double dev = value - mean;
            mean += dev / count;
            sumOfSquaredDeviation += dev * (value - mean);

            int i = getRow(index);
            int j = getCol(index);
            if (bucketCounts[i] == null) {
                bucketCounts[i] = new long[8];
            }
            if (bucketCounts[i][j] == 0) {
                numBuckets++;
            }
            bucketCounts[i][j] += 1;
        }
    }

    private int findBucketNumber(long value) {
        if (value < 0) {
            return 0;
        }
        // Algo to find bucket has logarithmic complexity. Max number of
        // iterations is log_2 64. Note that it is logarithmic wrt number of
        // buckets and not the value
        int left = 0;
        int right = MetricValue.NUM_FINITE_BUCKETS;
        int mid;
        while (left != right) {
            mid = (left + right) / 2;
            if (value >> mid == 0) {
                right = mid;
            } else {
                left = mid + 1;
            }
        }
        return left + 1;
    }

    private int getRow(int index) {
        // index/8
        return index >> 3;
    }

    private int getCol(int index) {
        // index%8
        return index & 7;
    }

    public MetricValue getMetricValue(String metricName) {
        long bucketMask = 0;
        int i , j;
        synchronized (this) {
            long[] bucketCountsArr = new long[numBuckets];
            int bucketCountIndex = 0;
            for (int index = 0; index < 64 && bucketCountIndex < numBuckets; index++) {
                i = getRow(index);
                j = getCol(index);
                if (bucketCounts[i] != null && bucketCounts[i][j] > 0) {
                    bucketMask |= 1 << index;
                    bucketCountsArr[bucketCountIndex] = bucketCounts[i][j];
                    bucketCountIndex++;
                }
            }
            return new MetricValue(metricName, bucketCountsArr, bucketMask, mean, sumOfSquaredDeviation);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append((String.format("count: %d, numBuckets:%d, mean:%32.2f, sumOfSquaredDeviation:%32.2f %n",
                count, numBuckets, mean, sumOfSquaredDeviation)));
        for (int index = 0; index < 8; index++) {
            if (bucketCounts[index] != null) {
                sb.append(String.format("%d: %s%n", Arrays.toString(bucketCounts[index])));
            } else {
                sb.append(String.format("%d: null%n", index));
            }
        }
        return sb.toString();
    }
}
