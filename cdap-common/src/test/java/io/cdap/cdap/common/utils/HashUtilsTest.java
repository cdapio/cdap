/*
 * Copyright © 2023 Cask Data, Inc.
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

package io.cdap.cdap.common.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class HashUtilsTest {

  @Test
  public void testTimeBucketHash() {
    long currentTime = System.currentTimeMillis();
    int window = 15;
    Set<String> results1 = new HashSet<>();
    Set<String> results2 = new HashSet<>();
    long dayInMilliSec = TimeUnit.DAYS.toMillis(1);
    for (int i = 0; i < 15; i++) {
      results1.add(HashUtils.timeBucketHash("hash1", window, currentTime + (i * dayInMilliSec)));
      results2.add(HashUtils.timeBucketHash("hash2", window, currentTime + (i * dayInMilliSec)));
    }

    // for 15 days window, we should have maximum 2 time buckets
    Assert.assertTrue(results1.size() >= 1 && results1.size() <= 2);
    Assert.assertTrue(results2.size() >= 1 && results2.size() <= 2);

    for (String res : results1) {
      results2.remove(res);
    }

    for (String res : results2) {
      results1.remove(res);
    }

    // ensures that jitter works so all keys do not end up in the same bucket
    Assert.assertNotEquals(results1.size(), 0);
    Assert.assertNotEquals(results2.size(), 0);
  }
}
