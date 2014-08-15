/*
 * Copyright 2014 Cask, Inc.
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

package co.cask.cdap.common.collect;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;

public class CollectorTest {


  private Collection<Integer> collect(Collector<Integer> collector, int n) {
    for (int i = 0; i < n; i++) {
      if (!collector.addElement(i)) {
        break;
      }
    }
    return collector.finish(new ArrayList<Integer>());
  }

  @Test
  public void testAllCollector() {
    Collector<Integer> collector = new AllCollector<Integer>();
    Assert.assertEquals(collect(collector, 0), ImmutableList.<Integer>of());
    Assert.assertEquals(collect(collector, 4), ImmutableList.of(0, 1, 2, 3));
    Assert.assertEquals(collect(collector, 4), ImmutableList.of(0, 1, 2, 3));
  }

  @Test
  public void testFirstNCollector() {
    Collector<Integer> collector1 = new FirstNCollector<Integer>(1);
    Collector<Integer> collector4 = new FirstNCollector<Integer>(4);
    Collector<Integer> collector10 = new FirstNCollector<Integer>(10);

    // add 0 elements
    Assert.assertEquals(collect(collector1, 0), ImmutableList.<Integer>of());
    Assert.assertEquals(collect(collector10, 0), ImmutableList.<Integer>of());
    // add more than capacity
    Assert.assertEquals(collect(collector1, 10), ImmutableList.of(0));
    Assert.assertEquals(collect(collector4, 10), ImmutableList.of(0, 1, 2, 3));
    // add same as capacity
    Assert.assertEquals(collect(collector1, 1), ImmutableList.of(0));
    Assert.assertEquals(collect(collector4, 4), ImmutableList.of(0, 1, 2, 3));
    // add less than capacity
    Assert.assertEquals(collect(collector4, 1), ImmutableList.of(0));
    Assert.assertEquals(collect(collector10, 4), ImmutableList.of(0, 1, 2, 3));
  }

  @Test
  public void testLastNCollector() {
    Collector<Integer> collector1 = new LastNCollector<Integer>(1);
    Collector<Integer> collector4 = new LastNCollector<Integer>(4);
    Collector<Integer> collector10 = new LastNCollector<Integer>(10);

    // add 0 elements
    Assert.assertEquals(collect(collector1, 0), ImmutableList.<Integer>of());
    Assert.assertEquals(collect(collector10, 0), ImmutableList.<Integer>of());
    // add more than capacity
    Assert.assertEquals(collect(collector1, 10), ImmutableList.of(9));
    Assert.assertEquals(collect(collector4, 10), ImmutableList.of(6, 7, 8, 9));
    // add same as capacity
    Assert.assertEquals(collect(collector1, 1), ImmutableList.of(0));
    Assert.assertEquals(collect(collector4, 4), ImmutableList.of(0, 1, 2, 3));
    // add less than capacity
    Assert.assertEquals(collect(collector4, 1), ImmutableList.of(0));
    Assert.assertEquals(collect(collector10, 4), ImmutableList.of(0, 1, 2, 3));
  }

}
