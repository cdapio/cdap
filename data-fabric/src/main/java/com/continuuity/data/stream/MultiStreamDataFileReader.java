/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * A {@link StreamEventReadable} that combines multiple event stream into single event stream.
 */
public final class MultiStreamDataFileReader implements StreamEventReadable<Iterable<StreamOffset>> {

  private final PriorityQueue<StreamEventSource> eventSources;
  private final Set<StreamEventSource> emptySources;
  private final Set<StreamEventSource> allSources;
  private final List<StreamOffset> offsets;
  private final Iterable<StreamOffset> offsetsView;

  public MultiStreamDataFileReader(Collection<? extends StreamDataFileSource> sources) {
    this.eventSources = new ObjectHeapPriorityQueue<StreamEventSource>(sources.size());
    this.allSources = Sets.newHashSet();
    this.offsets = Lists.newArrayListWithCapacity(sources.size());
    this.offsetsView = Collections.unmodifiableCollection(offsets);

    for (StreamDataFileSource source : sources) {
      StreamEventSource eventSource = new StreamEventSource(source);
      allSources.add(eventSource);
      offsets.add(new DefaultStreamOffset(eventSource));
    }

    this.emptySources = Sets.newHashSet(allSources);
  }

  public int next(Collection<StreamEvent> events, int maxEvents,
                  long timeout, TimeUnit unit) throws IOException, InterruptedException {

    int eventsRead = 0;

    Stopwatch stopwatch = new Stopwatch();
    stopwatch.start();

    while (eventsRead < maxEvents && !(emptySources.isEmpty() && eventSources.isEmpty())) {
      if (!emptySources.isEmpty()) {
        prepareEmptySources();
      }
      eventsRead += read(events);

      if (stopwatch.elapsedTime(unit) > timeout) {
        break;
      }
    }

    return (eventsRead == 0 && emptySources.isEmpty() && eventSources.isEmpty()) ? -1 : eventsRead;
  }

  @Override
  public Iterable<StreamOffset> getOffset() {
    return offsetsView;
  }

  /**
   * For all sources that doesn't have any event buffered, try to read an event and put it in the priority queue
   * if event is available.
   */
  private void prepareEmptySources() throws IOException, InterruptedException {
    Iterator<StreamEventSource> iterator = emptySources.iterator();
    while (iterator.hasNext()) {
      StreamEventSource source = iterator.next();
      int len = source.prepare();
      if (len != 0) {
        iterator.remove();
        if (len > 0) {
          eventSources.enqueue(source);
        }
      }
    }
  }

  private int read(Collection<StreamEvent> events) throws IOException, InterruptedException {
    if (eventSources.isEmpty()) {
      return 0;
    }

    StreamEventSource source = eventSources.first();
    source.read(events);

    int res = source.prepare();
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

  private static final class DefaultStreamOffset implements StreamOffset {

    private final StreamEventSource eventSource;

    private DefaultStreamOffset(StreamEventSource eventSource) {
      this.eventSource = eventSource;
    }

    @Override
    public int getBucketId() {
      return eventSource.source.getBucketId();
    }

    @Override
    public int getBucketSequence() {
      return eventSource.source.getBucketSequence();
    }

    @Override
    public long getOffset() {
      return eventSource.getOffset();
    }
  }


  private static final class StreamEventSource implements Comparable<StreamEventSource>, Closeable {

    private final StreamDataFileSource source;
    private final List<StreamEvent> events;
    private long currentOffset;
    private long nextOffset;

    private StreamEventSource(StreamDataFileSource source) {
      this.source = source;
      this.events = Lists.newArrayListWithCapacity(1);
    }

    void read(Collection<StreamEvent> result) throws IOException, InterruptedException {
      result.add(events.get(0));
      events.clear();
      currentOffset = nextOffset;
    }

    long getOffset() {
      return currentOffset;
    }

    /**
     * Tries to read one event from the stream source.
     *
     * @return {@code 1} if an event is available from the source. 0
     * @throws IOException
     * @throws InterruptedException
     */
    int prepare() throws IOException, InterruptedException {
      if (events.isEmpty()) {
        int res = source.getReader().next(events, 1, 0L, TimeUnit.MILLISECONDS);
        this.nextOffset = source.getReader().getOffset();
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

      // Compare by bucketId
      cmp = Ints.compare(source.getBucketId(), other.source.getBucketId());
      if (cmp != 0) {
        return cmp;
      }

      // Compare by bucket sequence
      return Ints.compare(source.getBucketSequence(), other.source.getBucketSequence());
    }

    @Override
    public void close() throws IOException {
      source.getReader().close();
    }
  }
}
