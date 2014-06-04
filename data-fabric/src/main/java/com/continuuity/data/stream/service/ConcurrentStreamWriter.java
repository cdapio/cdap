/*
 * Copyright 2014 Continuuity,Inc. All Rights Reserved.
 */
package com.continuuity.data.stream.service;

import com.continuuity.api.flow.flowlet.StreamEvent;
import com.continuuity.api.stream.StreamEventData;
import com.continuuity.common.metrics.MetricsCollector;
import com.continuuity.common.stream.DefaultStreamEventData;
import com.continuuity.data.file.FileWriter;
import com.continuuity.data.stream.StreamCoordinator;
import com.continuuity.data.stream.StreamFileWriterFactory;
import com.continuuity.data.stream.StreamPropertyListener;
import com.continuuity.data.stream.StreamUtils;
import com.continuuity.data2.transaction.stream.StreamAdmin;
import com.continuuity.data2.transaction.stream.StreamConfig;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.twill.common.Cancellable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
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

  private final StreamCoordinator streamCoordinator;
  private final StreamAdmin streamAdmin;
  private final StreamMetaStore streamMetaStore;
  private final int workerThreads;
  private final MetricsCollector metricsCollector;
  private final ConcurrentMap<String, EventQueue> eventQueues;
  private final FileWriterSupplierFactory writerSupplierFactory;
  private final Set<String> generationWatched;
  private final List<Cancellable> cancellables;
  private final Lock createLock;

  public ConcurrentStreamWriter(StreamCoordinator streamCoordinator, StreamAdmin streamAdmin,
                                StreamMetaStore streamMetaStore, StreamFileWriterFactory writerFactory,
                                int workerThreads, MetricsCollector metricsCollector) {
    this.streamCoordinator = streamCoordinator;
    this.streamAdmin = streamAdmin;
    this.streamMetaStore = streamMetaStore;
    this.workerThreads = workerThreads;
    this.metricsCollector = metricsCollector;
    this.eventQueues = new MapMaker().concurrencyLevel(workerThreads).makeMap();
    this.writerSupplierFactory = new FileWriterSupplierFactory(writerFactory);
    this.generationWatched = Sets.newHashSet();
    this.cancellables = Lists.newArrayList();
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
    for (Cancellable cancellable : cancellables) {
      cancellable.cancel();
    }

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
      // Double check
      eventQueue = eventQueues.get(streamName);
      if (eventQueue != null) {
        return eventQueue;
      }

      if (!streamMetaStore.streamExists(accountId, streamName)) {
        throw new IllegalArgumentException("Stream not exists");
      }
      StreamUtils.ensureExists(streamAdmin, streamName);

      if (generationWatched.add(streamName)) {
        cancellables.add(streamCoordinator.addListener(streamName, writerSupplierFactory));
      }

      eventQueue = new EventQueue(streamName, writerSupplierFactory.create(streamName));
      eventQueues.put(streamName, eventQueue);

      return eventQueue;

    } catch (Exception e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw new IOException(e);
    } finally {
      createLock.unlock();
    }
  }

  /**
   * Factory for creating file writer supplier. It also watch for changes in stream generation so that
   * it can create appropriate file writer supplier.
   */
  private final class FileWriterSupplierFactory extends StreamPropertyListener {

    private final StreamFileWriterFactory writerFactory;
    private final Map<String, Integer> generations;

    FileWriterSupplierFactory(StreamFileWriterFactory writerFactory) {
      this.writerFactory = writerFactory;
      this.generations = Collections.synchronizedMap(Maps.<String, Integer>newHashMap());
    }

    @Override
    public void generationChanged(String streamName, int generation) {
      LOG.debug("Generation for stream '{}' changed to {} for stream writer", streamName, generation);
      generations.put(streamName, generation);

      EventQueue eventQueue = eventQueues.remove(streamName);
      if (eventQueue != null) {
        try {
          eventQueue.close();
        } catch (IOException e) {
          LOG.warn("Failed to close writer.", e);
        }
      }
    }

    @Override
    public void generationDeleted(String streamName) {
      // Generation deleted. Remove the cache.
      // This makes creation of file writer resort to scanning the stream directory for generation id.
      LOG.debug("Generation for stream '{}' deleted for stream writer", streamName);
      generations.remove(streamName);
    }

    Supplier<FileWriter<StreamEvent>> create(final String streamName) {
      return new Supplier<FileWriter<StreamEvent>>() {
        @Override
        public FileWriter<StreamEvent> get() {
          try {
            StreamConfig streamConfig = streamAdmin.getConfig(streamName);
            Integer generation = generations.get(streamName);
            if (generation == null) {
              generation = StreamUtils.getGeneration(streamConfig);
            }

            LOG.info("Create stream writer for {} with generation {}", streamName, generation);
            return writerFactory.create(streamConfig, generation);
          } catch (IOException e) {
            throw Throwables.propagate(e);
          }
        }
      };
    }
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
          writerSupplier.get().close();

          // Drain the queue with failure. This could happen when
          // 1. Shutting down of http service, which is fine to set to failure as all connections are closed already.
          // 2. When stream generation change. In this case, the client would received failure.
          HandlerStreamEventData data = queue.poll();
          while (data != null) {
            data.setState(HandlerStreamEventData.State.FAILURE);
            data = queue.poll();
          }
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
