/**
 * Copyright 2010 Sematext International
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
package com.continuuity.hbase.wd;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;

/**
 *
 */
public class OneByteSimpleHashTest {
  @Test
  public void testMaxDistribution() {
    RowKeyDistributorByHashPrefix.OneByteSimpleHash hasher = new RowKeyDistributorByHashPrefix.OneByteSimpleHash(256);
    byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
    Assert.assertEquals(256, allPrefixes.length);
    Random r = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] originalKey = new byte[3];
      r.nextBytes(originalKey);
      byte[] hash = hasher.getHashPrefix(originalKey);
      boolean found = false;
      for (int k = 0; k < allPrefixes.length; k++) {
        if (Arrays.equals(allPrefixes[k], hash)) {
          found = true;
          break;
        }
      }
      Assert.assertTrue("Hashed prefix wasn't found in all possible prefixes, val: " + Arrays.toString(hash), found);
    }

    Assert.assertArrayEquals(
            hasher.getHashPrefix(new byte[] {123, 12, 11}), hasher.getHashPrefix(new byte[] {123, 12, 11}));
  }

  @Test
  public void testLimitedDistribution() {
    RowKeyDistributorByHashPrefix.OneByteSimpleHash hasher = new RowKeyDistributorByHashPrefix.OneByteSimpleHash(10);
    byte[][] allPrefixes = hasher.getAllPossiblePrefixes();
    Assert.assertEquals(10, allPrefixes.length);
    Random r = new Random();
    for (int i = 0; i < 1000; i++) {
      byte[] originalKey = new byte[3];
      r.nextBytes(originalKey);
      byte[] hash = hasher.getHashPrefix(originalKey);
      boolean found = false;
      for (int k = 0; k < allPrefixes.length; k++) {
        if (Arrays.equals(allPrefixes[k], hash)) {
          found = true;
          break;
        }
      }
      Assert.assertTrue("Hashed prefix wasn't found in all possible prefixes, val: " + Arrays.toString(hash), found);
    }

    Assert.assertArrayEquals(
            hasher.getHashPrefix(new byte[] {123, 12, 11}), hasher.getHashPrefix(new byte[] {123, 12, 11}));
  }

  /**
   * Tests that records are well spread over buckets.
   * In fact this test-case verifies *even* distribution across buckets, which may be broken with changing the hashing
   * algorithm.
   */
  @Test
  public void testHashPrefixDistribution() {
    testDistribution(32, 55);
    testDistribution(37, 13);
    testDistribution(255, 20);
    testDistribution(256, 20);
    testDistribution(256, 1);
    testDistribution(1, 200);
    testDistribution(1, 1);
  }

  private void testDistribution(int maxBuckets, int countForEachBucket) {
    RowKeyDistributorByHashPrefix distributor =
      new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.OneByteSimpleHash(maxBuckets));
    int[] bucketCounts = new int[maxBuckets];
    for (int i = 0; i < maxBuckets * countForEachBucket; i++) {
      byte[] original = Bytes.toBytes(i);
      byte[] distributed = distributor.getDistributedKey(original);
      bucketCounts[distributed[0] & 0xff]++;
    }

    byte[][] allKeys = distributor.getAllDistributedKeys(new byte[0]);
    Assert.assertEquals(maxBuckets, allKeys.length);

    for (int bucketCount : bucketCounts) {
      // i.e. all buckets expected to have similar amount of values (+- 10%)
      Assert.assertTrue("Unexpected values count in bucket: " + bucketCount + ", avg: " + countForEachBucket,
              Math.abs((countForEachBucket - bucketCount) / countForEachBucket) < 0.10);
    }
  }
}
