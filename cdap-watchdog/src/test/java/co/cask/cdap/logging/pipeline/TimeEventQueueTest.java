/*
 * Copyright © 2017 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline;

import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/**
 * Unit-test for {@link TimeEventQueue}.
 */
public class TimeEventQueueTest {

  @Test
  public void testOrdering() {
    TimeEventQueue<TimestampedEvent, Integer> eventQueue = new TimeEventQueue<>(ImmutableSet.of(1, 3));
    List<TimestampedEvent> expected = new ArrayList<>();

    // Put 10 events to partition 1, with both increasing timestamps and offsets
    for (int i = 0; i < 10; i++) {
      TimestampedEvent event = new TimestampedEvent(i, "m1" + i);
      eventQueue.add(event, i, 10, 1, i);
      expected.add(event);
    }

    // Put 10 events to partition 3, with increasing offsets, but non-sorted, some duplicated timestamps
    for (int i = 0; i < 10; i++) {
      long timestamp = i % 3;
      TimestampedEvent event = new TimestampedEvent(timestamp, "m3" + i);
      eventQueue.add(event, timestamp, 10, 3, i);
      expected.add(event);
    }

    // Sort the expected result based on timestamp. Since Collections.sort is stable sort,
    // partition 1 events will always be before partition 3 if the timestamps are the same
    Collections.sort(expected, new Comparator<TimestampedEvent>() {
      @Override
      public int compare(TimestampedEvent o1, TimestampedEvent o2) {
        return Long.compare(o1.getTimestamp(), o2.getTimestamp());
      }
    });

    // Should have 20 events
    Assert.assertEquals(20, eventQueue.size());

    // Total size should be 20 * 10
    Assert.assertEquals(200, eventQueue.getEventSize());

    // When iterating the queue, it should gives event in timestamp order,
    // followed by partition order, followed by Kafka offset.
    Iterator<TimestampedEvent> expectedItor = expected.iterator();
    for (TimestampedEvent event : eventQueue) {
      Assert.assertTrue(expectedItor.hasNext());
      Assert.assertEquals(expectedItor.next(), event);
    }
  }

  @Test
  public void testKafkaOffset() {
    TimeEventQueue<String, Integer> eventQueue = new TimeEventQueue<>(ImmutableSet.of(1, 2));

    // Insert 6 events, with timestamps going back and forth
    eventQueue.add("m7", 7L, 10, 1, 0);
    eventQueue.add("m5", 5L, 10, 2, 1);
    eventQueue.add("m10", 10L, 10, 1, 2);
    eventQueue.add("m11", 11L, 10, 2, 3);
    eventQueue.add("m2", 2L, 10, 1, 4);
    eventQueue.add("m8", 8L, 10, 2, 5);

    // Have 6 events of size 10, so total size should be 60
    Assert.assertEquals(60, eventQueue.getEventSize());

    // Events should be time ordered when getting from iterator
    TimeEventQueue.EventIterator<String, Integer> iterator = eventQueue.iterator();

    Assert.assertEquals("m2", iterator.next());
    Assert.assertEquals(1, iterator.getPartition());
    Assert.assertEquals(4, iterator.getOffset().intValue());
    iterator.remove();
    Assert.assertEquals(50, eventQueue.getEventSize());
    Assert.assertEquals(0, eventQueue.getSmallestOffset(1).intValue());
    Assert.assertEquals(1, eventQueue.getSmallestOffset(2).intValue());

    Assert.assertEquals("m5", iterator.next());
    Assert.assertEquals(2, iterator.getPartition());
    Assert.assertEquals(1, iterator.getOffset().intValue());
    iterator.remove();
    Assert.assertEquals(40, eventQueue.getEventSize());
    Assert.assertEquals(0, eventQueue.getSmallestOffset(1).intValue());
    Assert.assertEquals(3, eventQueue.getSmallestOffset(2).intValue());

    Assert.assertEquals("m7", iterator.next());
    Assert.assertEquals(1, iterator.getPartition());
    Assert.assertEquals(0, iterator.getOffset().intValue());
    iterator.remove();
    Assert.assertEquals(30, eventQueue.getEventSize());
    Assert.assertEquals(2, eventQueue.getSmallestOffset(1).intValue());
    Assert.assertEquals(3, eventQueue.getSmallestOffset(2).intValue());

    Assert.assertEquals("m8", iterator.next());
    Assert.assertEquals(2, iterator.getPartition());
    Assert.assertEquals(5, iterator.getOffset().intValue());
    iterator.remove();
    Assert.assertEquals(20, eventQueue.getEventSize());
    Assert.assertEquals(2, eventQueue.getSmallestOffset(1).intValue());
    Assert.assertEquals(3, eventQueue.getSmallestOffset(2).intValue());

    Assert.assertEquals("m10", iterator.next());
    Assert.assertEquals(1, iterator.getPartition());
    Assert.assertEquals(2, iterator.getOffset().intValue());
    iterator.remove();
    Assert.assertEquals(10, eventQueue.getEventSize());

    Assert.assertEquals("m11", iterator.next());
    Assert.assertEquals(2, iterator.getPartition());
    Assert.assertEquals(3, iterator.getOffset().intValue());
    iterator.remove();
    Assert.assertEquals(0, eventQueue.getEventSize());

    Assert.assertTrue(eventQueue.isEmpty());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testInvalidPartition() {
    TimeEventQueue<String, Integer> eventQueue = new TimeEventQueue<>(Collections.singleton(1));
    eventQueue.add("test", 1L, 10, 2, 0);
  }

  @Test (expected = IllegalStateException.class)
  public void testIllegalRemove() {
    TimeEventQueue<String, Integer> eventQueue = new TimeEventQueue<>(Collections.singleton(1));
    eventQueue.add("test", 1L, 10, 1, 0);
    Iterator<String> iterator = eventQueue.iterator();
    iterator.next();
    iterator.remove();
    iterator.remove();
  }

  private static final class TimestampedEvent {
    private final long timestamp;
    private final String message;

    TimestampedEvent(long timestamp, String message) {
      this.timestamp = timestamp;
      this.message = message;
    }

    long getTimestamp() {
      return timestamp;
    }

    String getMessage() {
      return message;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      TimestampedEvent that = (TimestampedEvent) o;
      return timestamp == that.timestamp && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
      return Objects.hash(timestamp, message);
    }
  }
}
