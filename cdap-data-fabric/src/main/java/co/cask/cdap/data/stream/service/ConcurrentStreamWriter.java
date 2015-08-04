/*
 * Copyright © 2014-2015 Cask Data, Inc.
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
package co.cask.cdap.data.stream.service;

import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.stream.StreamEventData;
import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.data.file.FileWriter;
import co.cask.cdap.data.file.FileWriters;
import co.cask.cdap.data.stream.StreamCoordinatorClient;
import co.cask.cdap.data.stream.StreamDataFileConstants;
import co.cask.cdap.data.stream.StreamFileType;
import co.cask.cdap.data.stream.StreamFileWriterFactory;
import co.cask.cdap.data.stream.StreamPropertyListener;
import co.cask.cdap.data.stream.StreamUtils;
import co.cask.cdap.data.stream.TimestampCloseable;
import co.cask.cdap.data2.transaction.stream.StreamAdmin;
import co.cask.cdap.data2.transaction.stream.StreamConfig;
import co.cask.cdap.proto.Id;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.collect.MapMaker;
import com.google.common.collect.Sets;
import com.google.common.io.Closeables;
import org.apache.twill.common.Cancellable;
import org.apache.twill.filesystem.Location;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import javax.annotation.concurrent.ThreadSafe;

/**
 * Class to support writing to stream with high concurrency. This class supports writing individual stream events
 * as well as appending a new stream file to a stream.
 *
 * For writing individual events to stream, it uses a non-blocking algorithm to batch writes from concurrent threads.
 * The algorithm is like this:
 *
 * When a thread that received a request, for each stream, performs the following:
 *
 * <pre>
 * 1. Constructs a StreamEventData locally and enqueue it to a ConcurrentLinkedQueue.
 * 2. Use CAS to set an AtomicBoolean flag to true.
 * 3. If successfully set the flag to true, this thread becomes the writer and proceed to run step 4-7.
 * 4. Keep polling StreamEventData from the concurrent queue and write to FileWriter with the current timestamp until
 *    the queue is empty.
 * 5. Perform a writer flush to make sure all data written are persisted.
 * 6. Set the state of each StreamEventData that are written to COMPLETED (succeed/failure).
 * 7. Set the AtomicBoolean flag back to false.
 * 8. If the StreamEventData enqueued by this thread is NOT COMPLETED, go back to step 2.
 * </pre>
 *
 * The spin lock between step 2 to step 8 is necessary as it guarantees events enqueued by all threads would eventually
 * get written and flushed.
 *
 */
@ThreadSafe
public final class ConcurrentStreamWriter implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(ConcurrentStreamWriter.class);

  private final StreamCoordinatorClient streamCoordinatorClient;
  private final StreamAdmin streamAdmin;
  private final int workerThreads;
  private final StreamMetricsCollectorFactory metricsCollectorFactory;
  private final ConcurrentMap<Id.Stream, EventQueue> eventQueues;
  private final StreamFileFactory streamFileFactory;
  private final Set<Id.Stream> generationWatched;
  private final List<Cancellable> cancellables;
  private final Lock createLock;

  ConcurrentStreamWriter(StreamCoordinatorClient streamCoordinatorClient, StreamAdmin streamAdmin,
                         StreamFileWriterFactory writerFactory, int workerThreads,
                         StreamMetricsCollectorFactory metricsCollectorFactory) {
    this.streamCoordinatorClient = streamCoordinatorClient;
    this.streamAdmin = streamAdmin;
    this.workerThreads = workerThreads;
    this.metricsCollectorFactory = metricsCollectorFactory;
    this.eventQueues = new MapMaker().concurrencyLevel(workerThreads).makeMap();
    this.streamFileFactory = new StreamFileFactory(writerFactory);
    this.generationWatched = Sets.newHashSet();
    this.cancellables = Lists.newArrayList();
    this.createLock = new ReentrantLock();
  }

  /**
   * Writes an event to the given stream.
   *
   * @param streamId identifier of the stream
   * @param headers header of the event
   * @param body content of the event
   *
   * @throws IOException if failed to write to stream
   * @throws IllegalArgumentException If the stream doesn't exists
   */
  public void enqueue(Id.Stream streamId,
                      Map<String, String> headers, ByteBuffer body) throws IOException, NotFoundException {
    EventQueue eventQueue = getEventQueue(streamId);
    WriteRequest writeRequest = eventQueue.append(headers, body);
    persistUntilCompleted(streamId, eventQueue, writeRequest);
  }

  /**
   * Writes a list of events to the given stream.
   *
   * @param streamId identifier of the stream
   * @param events list of events to write
   * @throws IOException if failed to write to stream
   * @throws IllegalArgumentException If the stream doesn't exists
   */
  public void enqueue(Id.Stream streamId,
                      Iterator<? extends StreamEventData> events) throws IOException, NotFoundException {
    EventQueue eventQueue = getEventQueue(streamId);
    WriteRequest writeRequest = eventQueue.append(events);
    persistUntilCompleted(streamId, eventQueue, writeRequest);
  }

  /**
   * Writes an event to the given stream asynchronously. This method returns when the new event is stored to
   * the in-memory event queue, but before persisted.
   *
   * @param streamId identifier of the stream
   * @param headers header of the event
   * @param body content of the event
   * @param executor The executor for performing the async write flush operation
   * @throws IOException if fails to get stream information
   * @throws IllegalArgumentException If the stream doesn't exists
   */
  public void asyncEnqueue(final Id.Stream streamId,
                           Map<String, String> headers, ByteBuffer body,
                           Executor executor) throws IOException, NotFoundException {
    // Put the event to the queue first and then execute the write asynchronously
    final EventQueue eventQueue = getEventQueue(streamId);
    final WriteRequest writeRequest = eventQueue.append(headers, body);
    executor.execute(new Runnable() {
      @Override
      public void run() {
        try {
          persistUntilCompleted(streamId, eventQueue, writeRequest);
        } catch (IOException e) {
          // Since it's done in the async executor, simply log the exception
          LOG.error("Async write failed", e);
        }
      }
    });
  }

  /**
   * Appends a new stream file to the given stream.
   *
   * @param streamId identifier of the stream
   * @param eventFile location to the new stream data file
   * @param indexFile location to the new stream index file
   * @param eventCount number of events in the given stream file
   * @param timestampCloseable a {@link TimestampCloseable} to close and return the stream file close timestamp
   * @throws IOException if failed to append the new stream file
   */
  public void appendFile(Id.Stream streamId,
                         Location eventFile, Location indexFile, long eventCount,
                         TimestampCloseable timestampCloseable) throws IOException, NotFoundException {
    EventQueue eventQueue = getEventQueue(streamId);
    StreamConfig config = streamAdmin.getConfig(streamId);
    while (!eventQueue.tryAppendFile(config, eventFile, indexFile, eventCount, timestampCloseable)) {
      Thread.yield();
    }
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

  private EventQueue getEventQueue(Id.Stream streamId) throws IOException, NotFoundException {
    EventQueue eventQueue = eventQueues.get(streamId);
    if (eventQueue != null) {
      return eventQueue;
    }

    createLock.lock();
    try {
      // Double check
      eventQueue = eventQueues.get(streamId);
      if (eventQueue != null) {
        return eventQueue;
      }

      if (!streamAdmin.exists(streamId)) {
        throw new NotFoundException(streamId);
      }
      StreamUtils.ensureExists(streamAdmin, streamId);

      if (generationWatched.add(streamId)) {
        cancellables.add(streamCoordinatorClient.addListener(streamId, streamFileFactory));
      }

      eventQueue = new EventQueue(streamId, metricsCollectorFactory.createMetricsCollector(streamId));
      eventQueues.put(streamId, eventQueue);

      return eventQueue;

    } catch (NotFoundException | IOException e) {
      throw e;
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      createLock.unlock();
    }
  }

  /**
   * Persists events in the given eventQueue until the given request is completed.
   *
   * @param eventQueue the queue containing events that needs to be persisted
   * @param request a request for persisting data to stream
   * @throws IOException if failed to write to stream
   */
  private void persistUntilCompleted(Id.Stream streamId, EventQueue eventQueue, WriteRequest request)
    throws IOException {
    while (!request.isCompleted()) {
      if (!eventQueue.tryWrite()) {
        Thread.yield();
      }
    }
    if (!request.isSuccess()) {
      Throwables.propagateIfInstanceOf(request.getFailure(), IOException.class);
      throw new IOException("Unable to write stream event to " + streamId, request.getFailure());
    }
  }

  /**
   * Factory for creating stream file and stream {@link FileWriter}.
   * It also watch for changes in stream generation so that it can create appropriate file/file writer.
   */
  private final class StreamFileFactory extends StreamPropertyListener {

    private final StreamFileWriterFactory writerFactory;

    StreamFileFactory(StreamFileWriterFactory writerFactory) {
      this.writerFactory = writerFactory;
    }

    @Override
    public void generationChanged(Id.Stream streamId, int generation) {
      LOG.debug("Generation for stream '{}' changed to {} for stream writer", streamId, generation);
      closeEventQueue(streamId);
    }

    @Override
    public void deleted(Id.Stream streamId) {
      LOG.debug("Properties deleted for stream '{}' for stream writer", streamId);
      closeEventQueue(streamId);
    }

    private void closeEventQueue(Id.Stream streamId) {
      EventQueue eventQueue = eventQueues.remove(streamId);
      if (eventQueue != null) {
        try {
          eventQueue.close();
        } catch (IOException e) {
          LOG.warn("Failed to close writer.", e);
        }
      }
    }

    /**
     * Creates a new {@link FileWriter} for the given stream.
     *
     * @param streamId identifier of the stream
     * @return A {@link FileWriter} for writing {@link StreamEvent} to the given stream
     * @throws IOException if failed to create the file writer
     */
    FileWriter<StreamEvent> create(Id.Stream streamId) throws IOException {
      StreamConfig streamConfig = streamAdmin.getConfig(streamId);
      int generation = StreamUtils.getGeneration(streamConfig);

      LOG.info("Create stream writer for {} with generation {}", streamId, generation);
      return writerFactory.create(streamConfig, generation);
    }

    /**
     * Appends a stream file to the given stream. When this method completed successfully, the given event and index
     * file locations will be renamed (hence removed) to the final stream file locations. Note that only stream file
     * that are written with the {@link StreamDataFileConstants.Property.Key#UNI_TIMESTAMP} property set to
     * {@link StreamDataFileConstants.Property.Value#CLOSE_TIMESTAMP} should be appended, although this method
     * doesn't do explicit check (for performance reason).
     *
     * @param config configuration about the stream to append to
     * @param eventFile location of the new event file
     * @param indexFile location of the new index file
     * @param timestamp close timestamp of the stream file
     * @throws IOException if failed to append the file to the stream
     */
    void appendFile(StreamConfig config, Location eventFile, Location indexFile, long timestamp) throws IOException {
      int generation = StreamUtils.getGeneration(config);

      // Figure out the partition directory based on generation and timestamp
      Location baseLocation = StreamUtils.createGenerationLocation(config.getLocation(), generation);
      long partitionDuration = config.getPartitionDuration();
      long partitionStartTime = StreamUtils.getPartitionStartTime(timestamp, partitionDuration);
      Location partitionLocation = StreamUtils.createPartitionLocation(baseLocation,
                                                                       partitionStartTime, partitionDuration);
      partitionLocation.mkdirs();

      // Figure out the final stream file name
      String filePrefix = writerFactory.getFileNamePrefix();
      int fileSequence = StreamUtils.getNextSequenceId(partitionLocation, filePrefix);

      Location destEventFile = StreamUtils.createStreamLocation(partitionLocation, filePrefix,
                                                            fileSequence, StreamFileType.EVENT);
      Location destIndexFile = StreamUtils.createStreamLocation(partitionLocation, filePrefix,
                                                            fileSequence, StreamFileType.INDEX);
      // The creation should succeed, as it's expected to only have one process running per fileNamePrefix.
      if (!destEventFile.createNew() || !destIndexFile.createNew()) {
        throw new IOException(String.format("Failed to create new file at %s and %s",
                                            destEventFile.toURI(), destIndexFile.toURI()));
      }

      // Rename the index file first, then the event file
      indexFile.renameTo(destIndexFile);
      eventFile.renameTo(destEventFile);
    }
  }

  /**
   * For buffering StreamEvents and doing batch write to stream file.
   */
  private final class EventQueue implements Closeable {

    private final Id.Stream streamId;
    private final StreamMetricsCollectorFactory.StreamMetricsCollector metricsCollector;
    private final Queue<WriteRequest> queue;
    private final AtomicBoolean writerFlag;
    private final WriteRequest.Metrics metrics;
    private final MutableStreamEvent streamEvent;
    private final Function<StreamEventData, StreamEvent> eventTransformer;
    private FileWriter<StreamEventData> fileWriter;
    private boolean closed;

    EventQueue(Id.Stream streamId, StreamMetricsCollectorFactory.StreamMetricsCollector metricsCollector) {
      this.streamId = streamId;
      this.streamEvent = new MutableStreamEvent();
      this.queue = new ConcurrentLinkedQueue<>();
      this.writerFlag = new AtomicBoolean(false);
      this.metrics = new WriteRequest.Metrics();
      this.metricsCollector = metricsCollector;
      this.eventTransformer = new Function<StreamEventData, StreamEvent>() {
        @Override
        public StreamEvent apply(StreamEventData data) {
          return streamEvent.setData(data);
        }
      };
    }

    /**
     * Adds an event to the event queue.
     *
     * @param headers headers of the event
     * @param body body of the event
     * @return A {@link WriteRequest} that contains the status of the request
     */
    WriteRequest append(Map<String, String> headers, ByteBuffer body) {
      WriteRequest request = new SingleWriteRequest(headers, body);
      queue.add(request);
      return request;
    }

    /**
     * Adds a list of events to the event queue. All events provided by the {@link Iterator} will be written with the
     * same event timestamp and are guaranteed to be written in the same data block inside a stream file.
     *
     * @param events an {@link Iterator} of {@link StreamEventData} containing the list of events to be written
     * @return A {@link WriteRequest} that contains the status of the request
     */
    WriteRequest append(Iterator<? extends StreamEventData> events) {
      WriteRequest request = new BatchWriteRequest(events);
      queue.add(request);
      return request;
    }

    /**
     * Attempts to append a file to the stream.
     *
     * @param streamConfig current configuration for the stream
     * @param eventFile location to the new stream data file
     * @param indexFile location to the new stream index file
     * @param eventCount number of events recorded in the new stream file
     * @param timestampCloseable A {@link TimestampCloseable} to close
     *                           and acquire the close timestamp for the new stream file
     * @return true if able to be the leader and append the file, false otherwise
     * @throws IOException if became leader but failed to perform the append operation
     */
    boolean tryAppendFile(StreamConfig streamConfig, Location eventFile, Location indexFile, long eventCount,
                          TimestampCloseable timestampCloseable) throws IOException {
      if (!writerFlag.compareAndSet(false, true)) {
        return false;
      }

      long fileSize;
      try {
        if (closed) {
          throw new IOException("Stream writer already closed");
        }
        if (fileWriter != null) {
          fileWriter.close();
          fileWriter = null;
        }
        timestampCloseable.close();
        fileSize = eventFile.length();
        streamFileFactory.appendFile(streamConfig, eventFile, indexFile, timestampCloseable.getCloseTimestamp());
      } finally {
        writerFlag.set(false);
      }

      metricsCollector.emitMetrics(fileSize, eventCount);
      return true;
    }

    /**
     * Attempts to write the queued events into the underlying stream.
     *
     * @return true if become the writer leader and performed the write, false otherwise.
     */
    boolean tryWrite() {
      int bytesWritten = 0;
      int eventsWritten = 0;

      if (!writerFlag.compareAndSet(false, true)) {
        return false;
      }

      // The visibility of states mutation done while getting hold of the writerFlag,
      // is piggy back on the writerFlag atomic variable update in the finally block,
      // hence all states mutated will be visible to all threads after that.
      try {
        metrics.reset();
        List<WriteRequest> processQueue = Lists.newArrayListWithExpectedSize(workerThreads);
        try {
          FileWriter<StreamEventData> writer = getFileWriter();
          WriteRequest request = queue.poll();
          streamEvent.setTimestamp(System.currentTimeMillis());
          while (request != null) {
            processQueue.add(request);
            request.write(writer, metrics);
            request = queue.poll();
          }
          writer.flush();
          for (WriteRequest processed : processQueue) {
            processed.completed(null);
          }
          bytesWritten = metrics.bytesWritten;
          eventsWritten = metrics.eventsWritten;
        } catch (Throwable t) {
          // On exception, remove this EventQueue from the map and close this event queue
          eventQueues.remove(streamId, this);
          doClose();

          for (WriteRequest processed : processQueue) {
            processed.completed(t);
          }
        }
      } finally {
        writerFlag.set(false);
      }

      metricsCollector.emitMetrics(bytesWritten, eventsWritten);
      return true;
    }

    /**
     * Returns the current {@link FileWriter}. A new {@link FileWriter} will be created
     * if none existed yet. This method should only be called from the writer leader thread.
     */
    private FileWriter<StreamEventData> getFileWriter() throws IOException {
      if (closed) {
        throw new IOException("Stream writer already closed");
      }
      if (fileWriter == null) {
        fileWriter = FileWriters.transform(streamFileFactory.create(streamId), eventTransformer);
      }
      return fileWriter;
    }

    @Override
    public void close() throws IOException {
      if (closed) {
        return;
      }
      boolean done = false;
      while (!done) {
        if (!writerFlag.compareAndSet(false, true)) {
          Thread.yield();
          continue;
        }
        try {
          doClose();
        } finally {
          done = true;
          writerFlag.set(false);
        }
      }
    }

    private void doClose() {
      if (fileWriter != null) {
        Closeables.closeQuietly(fileWriter);
      }

      // Drain the queue with failure. This could happen when
      // 1. Shutting down of http service, which is fine to set to failure as all connections are closed already.
      // 2. When stream generation change. In this case, the client would received failure.
      WriteRequest data = queue.poll();
      Throwable writerClosedException = new IOException("Stream writer closed").fillInStackTrace();
      while (data != null) {
        data.completed(writerClosedException);
        data = queue.poll();
      }
      closed = true;
    }
  }

  /**
   * Represents an active write request.
   */
  private abstract static class WriteRequest {
    enum State {
      PENDING,
      COMPLETED
    }

    /**
     * A simple POJO for carrying metrics information.
     */
    static final class Metrics {
      int bytesWritten;
      int eventsWritten;

      void reset() {
        bytesWritten = eventsWritten = 0;
      }

      void increment(int bytesWritten) {
        this.bytesWritten += bytesWritten;
        eventsWritten++;
      }
    }

    private State state = State.PENDING;
    private Throwable failure;

    boolean isCompleted() {
      return state != State.PENDING;
    }

    boolean isSuccess() {
      return isCompleted() && (failure == null);
    }

    void completed(Throwable failure) {
      this.state = State.COMPLETED;
      this.failure = failure;
    }

    Throwable getFailure() {
      return failure;
    }

    /**
     * Writes the data contained in this request to the given file writer.
     *
     * @param writer the {@link FileWriter} for writing {@link StreamEventData}
     * @param metrics for updating metrics about the event written
     * @throws IOException if failed to write to file
     */
    abstract void write(FileWriter<StreamEventData> writer, Metrics metrics) throws IOException;
  }

  /**
   * A {@link WriteRequest} that contains one stream event.
   */
  private static final class SingleWriteRequest extends WriteRequest {

    private final StreamEventData eventData;

    SingleWriteRequest(Map<String, String> headers, ByteBuffer body) {
      this.eventData = new StreamEventData(headers, body);
    }

    @Override
    void write(FileWriter<StreamEventData> writer, Metrics metrics) throws IOException {
      metrics.increment(eventData.getBody().remaining());
      writer.append(eventData);
    }
  }

  /**
   * A {@link WriteRequest} that contains a list of stream events.
   */
  private static final class BatchWriteRequest extends WriteRequest implements Iterator<StreamEventData> {

    private final Iterator<? extends StreamEventData> events;
    private Metrics metrics;

    private BatchWriteRequest(Iterator<? extends StreamEventData> events) {
      this.events = events;
    }

    @Override
    void write(FileWriter<StreamEventData> writer, Metrics metrics) throws IOException {
      this.metrics = metrics;
      writer.appendAll(this);
    }

    @Override
    public boolean hasNext() {
      return events.hasNext();
    }

    @Override
    public StreamEventData next() {
      StreamEventData data = events.next();
      metrics.increment(data.getBody().remaining());
      return data;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException("Remove not supported");
    }
  }
}
