/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.data.file.FileReader;
import com.continuuity.data.file.PositionReporter;
import com.continuuity.data.file.ReadFilter;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link FileReader} that combines multiple event stream into single event stream.
 */
@NotThreadSafe
public final class MultiLiveStreamFileReader implements FileReader<StreamEventOffset, Iterable<StreamFileOffset>> {

  private final PriorityQueue<StreamEventSource> eventSources;
  private final Set<StreamEventSource> emptySources;
  private final Set<StreamEventSource> allSources;
  private final Iterable<StreamFileOffset> offsetsView;

  public MultiLiveStreamFileReader(StreamConfig streamConfig, Iterable<? extends StreamFileOffset> offsets) {
    this.allSources = Sets.newTreeSet();

    for (StreamFileOffset source : offsets) {
      StreamEventSource eventSource = new StreamEventSource(streamConfig, source);
      allSources.add(eventSource);
    }

    this.eventSources = new ObjectHeapPriorityQueue<StreamEventSource>(allSources.size());
    this.emptySources = Sets.newHashSet(allSources);
    this.offsetsView = Iterables.transform(allSources, new Function<StreamEventSource, StreamFileOffset>() {
      @Override
      public StreamFileOffset apply(StreamEventSource input) {
        return input.getPosition();
      }
    });
  }

  @Override
  public void initialize() throws IOException {
    for (StreamEventSource source : allSources) {
      source.initialize();
    }
  }

  @Override
  public int read(Collection<? super StreamEventOffset> events, int maxEvents,
                  long timeout, TimeUnit unit) throws IOException, InterruptedException {
    return read(events, maxEvents, timeout, unit, ReadFilter.ALWAYS_ACCEPT);
  }

  @Override
  public int read(Collection<? super StreamEventOffset> events, int maxEvents,
                  long timeout, TimeUnit unit, ReadFilter readFilter) throws IOException, InterruptedException {
    int eventsRead = 0;

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    while (eventsRead < maxEvents && !(emptySources.isEmpty() && eventSources.isEmpty())) {
      if (!emptySources.isEmpty()) {
        prepareEmptySources(readFilter);
      }
      eventsRead += read(events, readFilter);

      if (eventSources.isEmpty() && stopwatch.elapsedTime(unit) > timeout) {
        break;
      }
    }

    return (eventsRead == 0 && emptySources.isEmpty() && eventSources.isEmpty()) ? -1 : eventsRead;
  }

  @Override
  public Iterable<StreamFileOffset> getPosition() {
    return offsetsView;
  }

  /**
   * For all sources that doesn't have any event buffered, try to read an event and put it in the priority queue
   * if event is available.
   */
  private void prepareEmptySources(ReadFilter readFilter) throws IOException, InterruptedException {
    Iterator<StreamEventSource> iterator = emptySources.iterator();
    while (iterator.hasNext()) {
      StreamEventSource source = iterator.next();
      int len = source.prepare(readFilter);
      if (len != 0) {
        iterator.remove();
        if (len > 0) {
          eventSources.enqueue(source);
        }
      }
    }
  }

  private int read(Collection<? super StreamEventOffset> events,
                   ReadFilter readFilter) throws IOException, InterruptedException {
    if (eventSources.isEmpty()) {
      return 0;
    }

    StreamEventSource source = eventSources.first();
    source.read(events);

    int res = source.prepare(readFilter);
    if (res > 0) {
      eventSources.changed();
    } else if (res <= 0) {
      eventSources.dequeue();
      if (res == 0) {
        emptySources.add(source);
      }
    }

    return 1;
  }

  @Override
  public void close() throws IOException {
    for (StreamEventSource source : allSources) {
      source.close();
    }

    emptySources.clear();
    eventSources.clear();
  }

  private static final class StreamEventSource implements Comparable<StreamEventSource>,
                                                          Closeable, PositionReporter<StreamFileOffset> {

    private final FileReader<PositionStreamEvent, StreamFileOffset> reader;
    private final List<PositionStreamEvent> events;
    private StreamFileOffset currentOffset;
    private StreamFileOffset nextOffset;

    private StreamEventSource(StreamConfig streamConfig, StreamFileOffset beginOffset) {
      this.reader = new LiveStreamFileReader(streamConfig, beginOffset);
      this.events = Lists.newArrayListWithCapacity(1);
      this.currentOffset = new StreamFileOffset(beginOffset);
      this.nextOffset = beginOffset;
    }

    void initialize() throws IOException {
      reader.initialize();
      currentOffset = reader.getPosition();
    }

    void read(Collection<? super StreamEventOffset> result) throws IOException, InterruptedException {
      // Pop the cached event and use the event start position as the event offset being returned.
      PositionStreamEvent streamEvent = events.get(0);

      // Use nextOffset location to construct file offset
      // because the actual file location can only be determined by a read to a LiveFileReader,
      // hence located inside nextOffset
      StreamFileOffset resultOffset = new StreamFileOffset(nextOffset.getEventLocation(),
                                                           streamEvent.getStart(), nextOffset.getGeneration());
      result.add(new StreamEventOffset(streamEvent, resultOffset));
      events.clear();

      // Updates current offset information to be after the current event.
      currentOffset = nextOffset;
    }

    /**
     * Tries to read one event from the stream source.
     *
     * @return {@code 1} if an event is available from the source.
     *         {@code 0} if no event is available.
     *         {@code -1} if reached end of source.
     * @throws IOException
     * @throws InterruptedException
     */
    int prepare(ReadFilter readFilter) throws IOException, InterruptedException {
      if (events.isEmpty()) {
        int res = reader.read(events, 1, 0L, TimeUnit.MILLISECONDS, readFilter);
        nextOffset = reader.getPosition();
        return res;
      }
      return 1;
    }

    @Override
    public int compareTo(StreamEventSource other) {
      if (this == other) {
        return 0;
      }

      // No event always come first.
      long ts = events.isEmpty() ? 0L : events.get(0).getTimestamp();
      long otherTs = other.events.isEmpty() ? 0L : other.events.get(0).getTimestamp();

      // Compare by timestamp
      int cmp = Longs.compare(ts, otherTs);
      if (cmp != 0) {
        return cmp;
      }

      // Tie break by file path
      return getPosition().getEventLocation().toURI().compareTo(other.getPosition().getEventLocation().toURI());
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }

    @Override
    public StreamFileOffset getPosition() {
      return currentOffset;
    }
  }
}
