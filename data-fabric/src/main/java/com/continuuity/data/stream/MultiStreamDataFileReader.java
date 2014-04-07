/*
 * Copyright 2012-2013 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.common.io.Locations;
import com.continuuity.common.io.SeekableInputStream;
import com.continuuity.data.file.FileReader;
import com.google.common.base.Function;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.io.InputSupplier;
import com.google.common.primitives.Longs;
import it.unimi.dsi.fastutil.PriorityQueue;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;
import org.apache.twill.filesystem.Location;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link FileReader} that combines multiple event stream into single event stream.
 */
@NotThreadSafe
public final class MultiStreamDataFileReader implements FileReader<StreamEvent, Iterable<StreamFileOffset>> {

  private final PriorityQueue<StreamEventSource> eventSources;
  private final Set<StreamEventSource> emptySources;
  private final Set<StreamEventSource> allSources;
  private final Iterable<StreamFileOffset> offsetsView;

  public MultiStreamDataFileReader(Collection<? extends StreamFileOffset> sources,
                                   Function<StreamFileOffset, FileReader<StreamEvent, Long>> readerFactory) {
    this.eventSources = new ObjectHeapPriorityQueue<StreamEventSource>(sources.size());
    this.allSources = Sets.newHashSet();
    List<StreamFileOffset> offsets = Lists.newArrayListWithCapacity(sources.size());
    this.offsetsView = Collections.unmodifiableCollection(offsets);

    for (StreamFileOffset source : sources) {
      StreamEventSource eventSource = new StreamEventSource(source, readerFactory);
      allSources.add(eventSource);
      offsets.add(new DefaultStreamFileOffset(eventSource));
    }

    this.emptySources = Sets.newHashSet(allSources);
  }

  @Override
  public int read(Collection<? super StreamEvent> events, int maxEvents,
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
  public Iterable<StreamFileOffset> getPosition() {
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

  private int read(Collection<? super StreamEvent> events) throws IOException, InterruptedException {
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

  private static final class DefaultStreamFileOffset extends StreamFileOffset {

    private final StreamEventSource eventSource;

    private DefaultStreamFileOffset(StreamEventSource eventSource) {
      super(eventSource.getEventLocation(), eventSource.getIndexLocation());
      this.eventSource = eventSource;
    }

    @Override
    public long getOffset() {
      return eventSource.getOffset();
    }
  }


  private static final class StreamEventSource implements Comparable<StreamEventSource>, Closeable {

    private final Location eventLocation;
    private final Location indexLocation;
    private final String bucketName;
    private final FileReader<StreamEvent, Long> reader;
    private final List<StreamEvent> events;
    private long currentOffset;
    private long nextOffset;

    private StreamEventSource(StreamFileOffset startOffset,
                              Function<StreamFileOffset, FileReader<StreamEvent, Long>> readerFactory) {
      this.eventLocation = startOffset.getEventLocation();
      this.indexLocation = startOffset.getIndexLocation();
      this.bucketName = StreamUtils.getBucketName(eventLocation.getName());
      this.reader = readerFactory.apply(startOffset);
      this.events = Lists.newArrayListWithCapacity(1);
    }

    void read(Collection<? super StreamEvent> result) throws IOException, InterruptedException {
      result.add(events.get(0));
      events.clear();
      currentOffset = nextOffset;
    }

    long getOffset() {
      return currentOffset;
    }

    Location getEventLocation() {
      return eventLocation;
    }

    Location getIndexLocation() {
      return indexLocation;
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
    int prepare() throws IOException, InterruptedException {
      if (events.isEmpty()) {
        int res = reader.read(events, 1, 0L, TimeUnit.MILLISECONDS);
        this.nextOffset = reader.getPosition();
        return res;
      }
      return 1;
    }

    private InputSupplier<? extends SeekableInputStream> createInputSupplier(@Nullable Location location) {
      return location == null ? null : Locations.newInputSupplier(location);
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
      return bucketName.compareTo(other.bucketName);
    }

    @Override
    public void close() throws IOException {
      reader.close();
    }
  }
}
