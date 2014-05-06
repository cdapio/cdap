/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.stream.DefaultStreamEventData;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.io.Closeables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class to support writing to single stream file with high concurrency.
 *
 * For writing to stream, it uses a non-blocking algorithm to batch writes from concurrent threads.
 * The algorithm is like this:
 *
 * When a thread that received a request, for each stream, performs the following:
 *
 * 1. Constructs a StreamEventData locally and enqueue it to a ConcurrentLinkedQueue.
 * 2. Use CAS to set an AtomicBoolean flag to true.
 * 3. If successfully set the flag to true, this thread becomes the writer and proceed to run step 4-7.
 * 4. Keep polling StreamEventData from the concurrent queue and write to FileWriter with the current timestamp until
 *    the queue is empty.
 * 5. Perform a writer flush to make sure all data written are persisted.
 * 6. Set the state of each StreamEventData that are written to COMPLETED (succeed/failure).
 * 7. Set the AtomicBoolean flag back to false.
 * 8. If the StreamEventData enqueued by this thread is NOT COMPLETED, go back to step 2.
 *
 * The spin lock between step 2 to step 8 is necessary as it guarantees events enqueued by all threads would eventually
 * get written and flushed.
 *
 */
@ThreadSafe
public final class ConcurrentStreamWriter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentStreamWriter.class);

  private final StreamAdmin streamAdmin;
  private final StreamMetaStore streamMetaStore;
  private final StreamFileWriterFactory writerFactory;
  private final int workerThreads;
  private final MetricsCollector metricsCollector;
  private final ConcurrentMap<String, EventQueue> eventQueues;
  private final Lock createLock;

  public ConcurrentStreamWriter(StreamAdmin streamAdmin, StreamMetaStore streamMetaStore,
                                StreamFileWriterFactory writerFactory,
                                int workerThreads, MetricsCollector metricsCollector) {
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.writerFactory = writerFactory;
    this.workerThreads = workerThreads;
    this.metricsCollector = metricsCollector;
    this.eventQueues = new MapMaker().concurrencyLevel(workerThreads).makeMap();
    this.createLock = new ReentrantLock();
  }

  /**
   * Writes an event to the given stream.
   *
   * @param stream Name of the stream
   * @param headers header of the event
   * @param body content of the event
   *
   * @throws IOException if failed to write to stream
   * @throws java.lang.IllegalArgumentException If the stream doesn't exists
   */
  public boolean enqueue(String accountId, String stream,
                         Map<String, String> headers, ByteBuffer body) throws IOException {
    EventQueue eventQueue = getEventQueue(accountId, stream);
    HandlerStreamEventData event = eventQueue.add(headers, body);
    do {
      if (!eventQueue.tryWrite()) {
        Thread.yield();
      }
    } while (!event.isCompleted());

    return event.isSuccess();
  }

  @Override
  public void close() throws IOException {
    for (EventQueue queue : eventQueues.values()) {
      try {
        queue.close();
      } catch (IOException e) {
        LOG.warn("Failed to close writer.", e);
      }
    }
  }


  private EventQueue getEventQueue(String accountId, String streamName) throws IOException {
    EventQueue eventQueue = eventQueues.get(streamName);
    if (eventQueue != null) {
      return eventQueue;
    }

    createLock.lock();
    try {
      if (!streamMetaStore.streamExists(accountId, streamName)) {
        throw new IllegalArgumentException("Stream not exists");
      }
      StreamUtils.ensureExists(streamAdmin, streamName);
    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    } finally {
      createLock.unlock();
    }
    eventQueue = new EventQueue(streamName, createWriterSupplier(streamName));
    EventQueue oldQueue = eventQueues.putIfAbsent(streamName, eventQueue);

    return (oldQueue == null) ? eventQueue : oldQueue;
  }

  private Supplier<FileWriter<StreamEvent>> createWriterSupplier(final String streamName) {
    return new Supplier<FileWriter<StreamEvent>>() {
      @Override
      public FileWriter<StreamEvent> get() {
        try {
          return writerFactory.create(streamName);
        } catch (IOException e) {
          throw Throwables.propagate(e);
        }
      }
    };
  }

  /**
   * For buffering StreamEvents and doing batch write to stream file.
   */
  private final class EventQueue implements Closeable {

    private final String streamName;
    private final Supplier<FileWriter<StreamEvent>> writerSupplier;
    private final Queue<HandlerStreamEventData> queue;
    private final AtomicBoolean writerFlag;
    private final SettableStreamEvent streamEvent;

    EventQueue(String streamName, Supplier<FileWriter<StreamEvent>> writerSupplier) {
      this.streamName = streamName;
      this.writerSupplier = Suppliers.memoize(writerSupplier);
      this.queue = new ConcurrentLinkedQueue<HandlerStreamEventData>();
      this.writerFlag = new AtomicBoolean(false);
      this.streamEvent = new SettableStreamEvent();
    }

    HandlerStreamEventData add(Map<String, String> headers, ByteBuffer body) {
      HandlerStreamEventData eventData = new HandlerStreamEventData(headers, body);
      queue.add(eventData);
      return eventData;
    }

    /**
     * Attempts to write the queued events into the underlying stream.
     *
     * @return true if become the writer leader and performed the write, false otherwise.
     */
    boolean tryWrite() {
      if (!writerFlag.compareAndSet(false, true)) {
        return false;
      }

      // The visibility of states mutation done while getting hold of the writerFlag,
      // is piggy back on the writerFlag atomic variable update in the finally block,
      // hence all states mutated will be visible to all threads after that.
      int bytesWritten = 0;
      int eventsWritten = 0;
      List<HandlerStreamEventData> processQueue = Lists.newArrayListWithExpectedSize(workerThreads);
      try {
        FileWriter<StreamEvent> writer = writerSupplier.get();
        HandlerStreamEventData data = queue.poll();
        long timestamp = System.currentTimeMillis();
        while (data != null) {
          processQueue.add(data);
          writer.append(streamEvent.set(data, timestamp));
          data = queue.poll();
        }
        writer.flush();
        for (HandlerStreamEventData processed : processQueue) {
          processed.setState(HandlerStreamEventData.State.SUCCESS);
          bytesWritten += processed.getBody().remaining();
        }
        eventsWritten = processQueue.size();
      } catch (Throwable t) {
        LOG.error("Failed to write to file for stream {}.", streamName, t);
        // On exception, remove this EventQueue from the map and close the writer associated with this instance
        eventQueues.remove(streamName, this);
        Closeables.closeQuietly(writerSupplier.get());

        for (HandlerStreamEventData processed : processQueue) {
          processed.setState(HandlerStreamEventData.State.FAILURE);
        }
      } finally {
        writerFlag.set(false);
      }

      if (eventsWritten > 0) {
        metricsCollector.gauge("collect.events", eventsWritten, streamName);
        metricsCollector.gauge("collect.bytes", bytesWritten, streamName);
      }

      return true;
    }

    @Override
    public void close() throws IOException {
      boolean done = false;
      while (!done) {
        if (!writerFlag.compareAndSet(false, true)) {
          Thread.yield();
          continue;
        }
        try {
          // Close is only called from the handler destroy method, hence no need to worry about pending events
          // in the queue, as the http service already closed the connection, hence no guarantee on whether
          // the event is persisted or not.
          writerSupplier.get().close();
        } finally {
          done = true;
          writerFlag.set(false);
        }
      }
    }
  }

  /**
   * A {@link StreamEventData} that carry state on whether it's been written to the underlying stream file or not.
   */
  private static final class HandlerStreamEventData extends DefaultStreamEventData {

    /**
     * The possible state of the event data.
     */
    enum State {
      PENDING,
      SUCCESS,
      FAILURE
    }

    private State state;

    public HandlerStreamEventData(Map<String, String> headers, ByteBuffer body) {
      super(headers, body);
      this.state = State.PENDING;
    }

    public boolean isCompleted() {
      return state != State.PENDING;
    }

    public boolean isSuccess() {
      return state == State.SUCCESS;
    }

    public void setState(State state) {
      this.state = state;
    }
  }

  /**
   * A mutable {@link StreamEvent} that allows setting the data and timestamp. Used by the writer thread
   * to save object creation. It doesn't need to be thread safe as there would be used by the active writer thread
   * only.
   *
   * @see StreamHandler
   */
  private static final class SettableStreamEvent implements StreamEvent {

    private StreamEventData data;
    private long timestamp;

    /**
     * Sets the event data and timestamp.

     * @return this instance.
     */
    public StreamEvent set(StreamEventData data, long timestamp) {
      this.data = data;
      this.timestamp = timestamp;
      return this;
    }

    @Override
    public long getTimestamp() {
      return timestamp;
    }

    @Override
    public ByteBuffer getBody() {
      return data.getBody();
    }

    @Override
    public Map<String, String> getHeaders() {
      return data.getHeaders();
    }
  }
}
