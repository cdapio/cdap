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

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class GaugeStatsTest {

  @Test
  public void test() {
    GaugeStats stats = new GaugeStats();

    stats.gauge(10);

    Assert.assertEquals(10.0f, stats.getSum(), 1.0f);
    Assert.assertEquals(1, stats.getCount());
    Assert.assertEquals(10, stats.getMin(), 1.0f);
    Assert.assertEquals(10, stats.getMax(), 1.0f);

    stats.gauge(20);

    Assert.assertEquals(30, stats.getSum(), 1.0f);
    Assert.assertEquals(2, stats.getCount());
    Assert.assertEquals(10, stats.getMin(), 1.0f);
    Assert.assertEquals(20, stats.getMax(), 1.0f);

    stats.gauge(30);

    Assert.assertEquals(60.0f, stats.getSum(), 1.0f);
    Assert.assertEquals(3, stats.getCount());
    Assert.assertEquals(10, stats.getMin(), 1.0f);
    Assert.assertEquals(30, stats.getMax(), 1.0f);

    stats.gauge(30);

    Assert.assertEquals(90.0f, stats.getSum(), 1.0f);
    Assert.assertEquals(4, stats.getCount());
    Assert.assertEquals(10, stats.getMin(), 1.0f);
    Assert.assertEquals(30, stats.getMax(), 1.0f);

  }
}
