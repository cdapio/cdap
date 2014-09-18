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
package co.cask.cdap.metrics.data;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 *
 */
public class TimeValueInterpolatedAggregatorTest {

  /**
   * Given two series that look like:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   -   4   -   6   -   8
   *   1   -   3   -   5   -   7   -
   *
   * The step interpolation, the individual time series get transformed into:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   2   4   4   6   6   8
   *   1   1   3   3   5   5   7   -
   *
   * and the final aggregate timeseries becomes:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   1   3   5   7   9   11  13  8
   */
  @Test
  public void testStepInterpolatedAggregator() {
    List<List<TimeValue>> list = generateInput();

    TimeValueInterpolatedAggregator aggregator =
      new TimeValueInterpolatedAggregator(list, new Interpolators.Step());
    int numPoints = 0;
    for (TimeValue tv : aggregator) {
      // first point doesn't follow the same formula, check it separately.
      if (tv.getTime() == 8) {
        Assert.assertEquals(8, tv.getValue());
      } else {
        Assert.assertEquals(2 * tv.getTime() - 1, tv.getValue());
      }
      numPoints++;
    }
    Assert.assertEquals(8, numPoints);
  }


  /**
   * Given two series that look like:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   -   4   -   6   -   8
   *   1   -   3   -   5   -   7   -
   *
   * The step interpolation, the individual time series get transformed into:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   -   2   3   4   5   6   7   8
   *   1   2   3   4   5   6   7   -
   *
   * and the final aggregate timeseries becomes:
   *   t1  t2  t3  t4  t5  t6  t7  t8
   *   1   4   6   8   10  12  14  8
   */
  @Test
  public void testLinearInterpolatedAggregator() {
    List<List<TimeValue>> list = generateInput();

    TimeValueInterpolatedAggregator aggregator =
      new TimeValueInterpolatedAggregator(list, new Interpolators.Linear());
    int numPoints = 0;
    for (TimeValue tv : aggregator) {
      // timestamps 1 and 8 only has one datapoint, so it doesn't follow the pattern.
      if (tv.getTime() == 1L || tv.getTime() == 8L) {
        Assert.assertEquals(tv.getValue(), tv.getTime());
      } else {
        Assert.assertEquals(2 * tv.getTime(), tv.getValue());
      }
      numPoints++;
    }
    Assert.assertEquals(8, numPoints);
  }

  private List<List<TimeValue>> generateInput() {
    List<List<TimeValue>> list = Lists.newArrayList();

    // even numbers
    List<TimeValue> timeseries1 = Lists.newLinkedList();
    for (int i = 2; i <= 8; i += 2) {
      timeseries1.add(new TimeValue(i, i));
    }
    list.add(timeseries1);

    // odd numbers
    List<TimeValue> timeseries2 = Lists.newLinkedList();
    for (int i = 1; i <= 7; i += 2) {
      timeseries2.add(new TimeValue(i, i));
    }
    list.add(timeseries2);
    return list;
  }
}
