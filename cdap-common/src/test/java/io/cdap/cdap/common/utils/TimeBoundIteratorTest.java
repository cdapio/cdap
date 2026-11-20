/*
 * Copyright © 2018-2024 Cask Data, Inc.
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

import com.google.common.base.Stopwatch;
import com.google.common.base.Ticker;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests {@link TimeBoundIterator}.
 */
public class TimeBoundIteratorTest {

  @Test
  public void testTimeBoundNotHit() {
    SettableTicker ticker = new SettableTicker(0);
    Stopwatch stopwatch = new Stopwatch(ticker);

    List<Integer> list = new ArrayList<>();
    list.add(0);
    list.add(1);
    list.add(2);

    Iterator<Integer> iter = new TimeBoundIterator<>(list.iterator(), Long.MAX_VALUE, stopwatch);
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(0, (int) iter.next());
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(1, (int) iter.next());
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(2, (int) iter.next());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testTimeBoundImmediatelyHit() {
    SettableTicker ticker = new SettableTicker(0);
    Stopwatch stopwatch = new Stopwatch(ticker);

    List<Integer> list = new ArrayList<>();
    list.add(0);
    list.add(1);
    list.add(2);

    Iterator<Integer> iter = new TimeBoundIterator<>(list.iterator(), 10, stopwatch);
    ticker.millis = 10;
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testEarlyStop() {
    SettableTicker ticker = new SettableTicker(0);
    Stopwatch stopwatch = new Stopwatch(ticker);

    List<Integer> list = new ArrayList<>();
    list.add(0);
    list.add(1);
    list.add(2);

    Iterator<Integer> iter = new TimeBoundIterator<>(list.iterator(), 10, stopwatch);
    ticker.millis = 9;
    Assert.assertTrue(iter.hasNext());
    Assert.assertEquals(0, (int) iter.next());
    Assert.assertTrue(iter.hasNext());
    ticker.millis = 10;
    Assert.assertEquals(1, (int) iter.next());
    Assert.assertFalse(iter.hasNext());
  }

  @Test
  public void testTwoArgConstructorAndRunningStopwatch() {
    List<String> items = Arrays.asList("a", "b");
    Iterator<String> generousIter = new TimeBoundIterator<>(items.iterator(), 60000L);
    Assert.assertTrue(generousIter.hasNext());
    Assert.assertEquals("a", generousIter.next());
    Assert.assertEquals("b", generousIter.next());
    Assert.assertFalse(generousIter.hasNext());

    Iterator<String> zeroBoundIter = new TimeBoundIterator<>(items.iterator(), 0L);
    Assert.assertFalse(zeroBoundIter.hasNext());

    SettableTicker ticker = new SettableTicker(5);
    Stopwatch alreadyStarted = new Stopwatch(ticker).start();
    Iterator<String> startedIter = new TimeBoundIterator<>(items.iterator(), 20L, alreadyStarted);
    Assert.assertTrue(startedIter.hasNext());
    Assert.assertEquals("a", startedIter.next());
  }

  /**
   * Ticker for unit tests.
   */
  private static class SettableTicker extends Ticker {
    private long millis;

    SettableTicker(long millis) {
      this.millis = millis;
    }

    @Override
    public long read() {
      return TimeUnit.MILLISECONDS.toNanos(millis);
    }
  }
}
