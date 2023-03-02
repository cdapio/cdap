/*
 * Copyright 2012 Cask Data, Inc.
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

import org.junit.Assert;
import org.junit.Test;

public class MetricValueTest {
  @Test
  public void testGetAllCounts() {
    long bucketMask = 1L /* first position */
        + 1L << 2 /* 2^2 i.e. third position */
        + 1L << 8 /* 2^8 i.e. 9th position */
        + 1L << 20 /* 2^20 i.e. 21st position */;

    MetricValue metricValue = new MetricValue("ignore", new long[]{1, 1, 1, 1}, bucketMask, 0);
    long[] allBucketCounts = metricValue.getAllBucketCounts();
    Assert.assertEquals(MetricValue.NUM_FINITE_BUCKETS + 2, allBucketCounts.length);
    for (int i = 0; i < allBucketCounts.length; i++) {
      if ((bucketMask & (1L << i)) != 0) {
        Assert.assertEquals(1, allBucketCounts[i]);
      } else {
        Assert.assertEquals(0, allBucketCounts[i]);
      }
    }
  }
}
