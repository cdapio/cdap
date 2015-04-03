/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.metrics.stats;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import java.math.BigInteger;

/**
 * Computes statistics for a gauge.
 */
public class GaugeStats {

  private Multiset<Long> values;

  private long min;
  private long max;
  private long count;

  public GaugeStats() {
    values = HashMultiset.create();
    count = 0;
    min = Long.MAX_VALUE;
    max = Long.MIN_VALUE;
  }

  public void gauge(long value) {
    values.add(value);
    min = Math.min(min, value);
    max = Math.max(max, value);
    count++;
  }

  public long getCount() {
    return count;
  }

  public long getAverage() {
    if (count == 0) {
      return 0;
    }

    double average = 0;
    for (Long value : values.elementSet()) {
      int valueCount = values.count(value);
      average += value * (valueCount * 1.0 / count);
    }

    return (long) average;
  }

  public long getMin() {
    return min;
  }

  public long getMax() {
    return max;
  }

  public boolean isEmpty() {
    return count == 0;
  }
}
