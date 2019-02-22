/*
 * Copyright © 2019 Cask Data, Inc.
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

package co.cask.cdap.logging.pipeline.queue;

import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;

import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A queue for storing time based events with offsets association.
 *
 * @param <EVENT> Type of event stored in the queue.
 * @param <OFFSET> Type of event offset associated with the event.
 */
@NotThreadSafe
public final class TimeEventQueue<EVENT, OFFSET extends Comparable<OFFSET>> implements Iterable<EVENT> {

  private final SortedSet<TimeEvent<EVENT, OFFSET>> events;
  private final Int2ObjectMap<SortedSet<OFFSET>> partitionOffsets;
  private long totalSize;

  public TimeEventQueue(Iterable<Integer> partitions) {
    this.events = new TreeSet<>();
    this.partitionOffsets = new Int2ObjectArrayMap<>();

    for (int partition : partitions) {
      partitionOffsets.put(partition, new TreeSet<>());
    }
  }

  public void add(EVENT event, long eventTimestamp, int eventSize, int partition, OFFSET offset) {
    SortedSet<OFFSET> offsets = getOffsets(partition);
    TimeEvent<EVENT, OFFSET> timeEvent = new TimeEvent<>(eventTimestamp, partition, offset, event, eventSize);
    if (events.add(timeEvent)) {
      if (!offsets.add(offset)) {
        events.remove(timeEvent);
        throw new IllegalArgumentException("Adding different event with the same offset "
                                             + offset + ", " + event);
      }

      totalSize += eventSize;
    }
  }

  /**
   * Returns the event in the queue with the smallest timestamp.
   */
  public EVENT first() {
    return events.first().getEvent();
  }

  /**
   * Returns {@code true} if there is no event in the queue.
   */
  public boolean isEmpty() {
    return events.isEmpty();
  }

  /**
   * Returns {@code true} if there is no event for the given partition in the queue.
   */
  public boolean isEmpty(int partition) {
    return getOffsets(partition).isEmpty();
  }

  /**
   * Returns the number of events in the queue.
   */
  public int size() {
    return events.size();
  }

  /**
   * Returns the size of all events in the queue.
   */
  public long getEventSize() {
    return totalSize;
  }

  /**
   * Returns the smallest offset stored for the given partition.
   */
  public OFFSET getSmallestOffset(int partition) {
    SortedSet<OFFSET> offsets = getOffsets(partition);
    if (offsets.isEmpty()) {
      throw new IllegalStateException("Queue is empty");
    }
    return offsets.first();
  }

  @Override
  public EventIterator<EVENT, OFFSET> iterator() {
    final Iterator<TimeEvent<EVENT, OFFSET>> iterator = events.iterator();
    return new EventIterator<EVENT, OFFSET>() {

      private TimeEvent<EVENT, OFFSET> currentEvent;

      @Override
      public boolean hasNext() {
        return iterator.hasNext();
      }

      @Override
      public EVENT next() {
        currentEvent = iterator.next();
        return currentEvent.getEvent();
      }

      @Override
      public void remove() {
        if (currentEvent == null) {
          throw new IllegalStateException("The next() method must be called first.");
        }
        iterator.remove();
        partitionOffsets.get(currentEvent.getPartition()).remove(currentEvent.getOffset());
        totalSize -= currentEvent.getEventSize();
        currentEvent = null;
      }

      @Override
      public OFFSET getOffset() {
        if (currentEvent == null) {
          throw new IllegalStateException("The next() method must be called first.");
        }
        return currentEvent.getOffset();
      }

      @Override
      public int getPartition() {
        if (currentEvent == null) {
          throw new IllegalStateException("The next() method must be called first.");
        }
        return currentEvent.getPartition();
      }
    };
  }

  private SortedSet<OFFSET> getOffsets(int partition) {
    SortedSet<OFFSET> offsets = partitionOffsets.get(partition);
    if (offsets == null) {
      throw new IllegalArgumentException("Partition " + partition +
                                           " is not in allowed partitions " + partitionOffsets.keySet());
    }
    return offsets;
  }

  /**
   * An {@link Iterator} for iterating over events inserted to the {@link TimeEventQueue}.
   *
   * @param <EVENT> type of element
   * @param <OFFSET> Type of event offset associated with the event.
   */
  public interface EventIterator<EVENT, OFFSET> extends Iterator<EVENT> {

    /**
     * Returns the offset provided at the insertion time of the last element returned by this iterator.
     */
    OFFSET getOffset();

    /**
     * Returns the partition provided at the insertion time of the last element returned by this iterator.
     */
    int getPartition();
  }

  /**
   * This class represent an event stored in the event set.
   */
  private static final class TimeEvent<EVENT, OFFSET extends Comparable<OFFSET>>
                        implements Comparable<TimeEvent<EVENT, OFFSET>> {
    private final long eventTime;
    private final int partition;
    private final OFFSET offset;
    private final EVENT event;
    private final int eventSize;

    TimeEvent(long eventTime, int partition, OFFSET offset, EVENT event, int eventSize) {
      this.eventTime = eventTime;
      this.partition = partition;
      this.offset = offset;
      this.event = event;
      this.eventSize = eventSize;
    }

    long getEventTime() {
      return eventTime;
    }

    int getPartition() {
      return partition;
    }

    OFFSET getOffset() {
      return offset;
    }

    EVENT getEvent() {
      return event;
    }

    int getEventSize() {
      return eventSize;
    }

    @Override
    public int compareTo(TimeEvent<EVENT, OFFSET> other) {
      // Compare by event time, then by partition, then by offset
      // Combination of them are guaranteed to be unique.

      int cmp = Long.compare(eventTime, other.getEventTime());
      if (cmp != 0) {
        return cmp;
      }
      cmp = Integer.compare(partition, other.getPartition());
      if (cmp != 0) {
        return cmp;
      }
      return offset.compareTo(other.getOffset());
    }
  }
}
