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

package co.cask.cdap.logging.pipeline.kafka;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.TimeEventQueue;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest$;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.OffsetOutOfRangeException;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * A log processing pipeline that reads from Kafka and writes to configured logger context.
 */
public final class KafkaLogProcessorPipeline extends AbstractExecutionThreadService {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaLogProcessorPipeline.class);
  private static final int KAFKA_SO_TIMEOUT = 3000;
  private static final double MIN_FREE_FACTOR = 0.5d;

  private final String name;
  private final LogProcessorPipelineContext context;
  private final CheckpointManager checkpointManager;
  private final BrokerService brokerService;
  private final Int2LongMap offsets;
  private final Int2ObjectMap<MutableCheckpoint> checkpoints;
  private final LoggingEventSerializer serializer;
  private final KafkaPipelineConfig config;
  private final TimeEventQueue<ILoggingEvent, Long> eventQueue;
  private final Map<BrokerInfo, KafkaSimpleConsumer> kafkaConsumers;

  private ExecutorService fetchExecutor;
  private volatile Thread runThread;
  private volatile boolean stopped;
  private long lastCheckpointTime;
  private int unSyncedEvents;

  public KafkaLogProcessorPipeline(LogProcessorPipelineContext context, CheckpointManager checkpointManager,
                                   BrokerService brokerService, KafkaPipelineConfig config) {
    this.name = context.getName();
    this.context = context;
    this.checkpointManager = checkpointManager;
    this.brokerService = brokerService;
    this.config = config;
    this.offsets = new Int2LongOpenHashMap();
    this.checkpoints = new Int2ObjectOpenHashMap<>();
    this.eventQueue = new TimeEventQueue<>(config.getPartitions());
    this.serializer = new LoggingEventSerializer();
    this.kafkaConsumers = new HashMap<>();
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting log processor pipeline for {} with configurations {}", name, config);

    // Reads the existing checkpoints
    Set<Integer> partitions = config.getPartitions();
    for (Map.Entry<Integer, Checkpoint> entry : checkpointManager.getCheckpoint(partitions).entrySet()) {
      Checkpoint checkpoint = entry.getValue();
      // Skip the partition that doesn't have previous checkpoint.
      if (checkpoint.getNextOffset() >= 0 && checkpoint.getMaxEventTime() >= 0) {
        checkpoints.put(entry.getKey(), new MutableCheckpoint(checkpoint));
      }
    }

    context.start();

    fetchExecutor = Executors.newFixedThreadPool(
      partitions.size(), Threads.createDaemonThreadFactory("fetcher-" + name + "-%d"));

    LOG.info("Log processor pipeline for {} with config {} started with checkpoint {}", name, config, checkpoints);
  }

  @Override
  protected void run() {
    runThread = Thread.currentThread();

    try {
      initializeOffsets();
      LOG.info("Kafka offsets initialize for pipeline {} as {}", name, offsets);

      Map<Integer, Future<Iterable<MessageAndOffset>>> futures = new HashMap<>();
      String topic = config.getTopic();

      lastCheckpointTime = System.currentTimeMillis();

      while (!stopped) {
        boolean hasMessageProcessed = false;

        for (Map.Entry<Integer, Future<Iterable<MessageAndOffset>>> entry : fetchAll(offsets, futures).entrySet()) {
          int partition = entry.getKey();
          try {
            if (processMessages(topic, partition, entry.getValue())) {
              hasMessageProcessed = true;
            }
          } catch (IOException e) {
            LOG.warn("Failed to process messages fetched from {}:{} due to {}. Will be retried in next iteration.",
                     topic, partition, e.getMessage());
            LOG.debug("Failed to process messages fetched from {}:{}", topic, partition, e);
          }
        }

        long now = System.currentTimeMillis();
        unSyncedEvents += appendEvents(now, false);
        long nextCheckpointDelay = trySyncAndPersistCheckpoints(now);

        // If nothing has been processed (e.g. empty fetch from Kafka, fail to append anything to appender),
        // Sleep until the earliest event in the buffer is time to be written out.
        if (!hasMessageProcessed) {
          long sleepMillis = config.getEventDelayMillis();
          if (!eventQueue.isEmpty()) {
            sleepMillis += eventQueue.first().getTimeStamp() - now;
          }
          if (sleepMillis > 0) {
            TimeUnit.MILLISECONDS.sleep(Math.min(sleepMillis, nextCheckpointDelay));
          }
        }
      }
    } catch (InterruptedException e) {
      // Interruption means stopping the service.
    }
  }

  @Override
  protected void triggerShutdown() {
    stopped = true;
    if (runThread != null) {
      runThread.interrupt();
    }
  }

  @Override
  protected void shutDown() throws Exception {
    LOG.debug("Shutting down log processor pipeline for {}", name);
    fetchExecutor.shutdownNow();

    try {
      context.stop();
      // Persist the checkpoints. It can only be done after successfully stopping the appenders.
      // Since persistCheckpoint never throw, putting it inside try is ok.
      persistCheckpoints();
    } catch (Exception e) {
      // Just log, not to fail the shutdown
      LOG.warn("Exception raised when stopping pipeline {}", name, e);
    }

    for (SimpleConsumer consumer : kafkaConsumers.values()) {
      try {
        consumer.close();
      } catch (Exception e) {
        // Just log, not to fail the shutdown
        LOG.warn("Exception raised when closing Kafka consumer.", e);
      }
    }
    LOG.info("Log processor pipeline for {} stopped with latest checkpoints {}", name, checkpoints);
  }

  @Override
  protected String getServiceName() {
    return "LogPipeline-" + name;
  }

  /**
   * Initialize offsets for all partitions consumed by this pipeline.
   *
   * @throws InterruptedException if there is an interruption
   */
  private void initializeOffsets() throws InterruptedException {
    // Setup initial offsets
    Set<Integer> partitions = config.getPartitions();
    while (offsets.size() != partitions.size() && !stopped) {
      for (int partition : partitions) {
        Checkpoint checkpoint = checkpoints.get(partition);
        if (checkpoint != null) {
          // TODO: (CDAP-7684) Deal with wrong offset issue
          offsets.put(partition, checkpoint.getNextOffset());
          continue;
        }

        try {
          // If no checkpoint, fetch from the beginning.
          offsets.put(partition, getLastOffset(partition, kafka.api.OffsetRequest.EarliestTime()));
        } catch (Exception e) {
          LOG.info("Failed to get Kafka earliest offset in {}:{} for pipeline {}. Will be retried",
                   config.getTopic(), partition, name);
          LOG.debug("Failed to get Kafka earliest offset in {}:{} for pipeline {}.",
                    config.getTopic(), partition, name, e);
          TimeUnit.SECONDS.sleep(1);
          break;
        }
      }
    }
  }

  /**
   * Process messages fetched from a given partition.
   */
  private boolean processMessages(String topic, int partition,
                                  Future<Iterable<MessageAndOffset>> future) throws InterruptedException, IOException {
    Iterable<MessageAndOffset> messages;
    try {
      messages = future.get();
    } catch (ExecutionException e) {
      try {
        throw e.getCause();
      } catch (OffsetOutOfRangeException cause) {
        // This shouldn't happen under normal situation.
        // If happened, usually is caused by race between kafka log rotation and fetching in here,
        // hence just fetching from the beginning should be fine
        offsets.put(partition, getLastOffset(partition, kafka.api.OffsetRequest.EarliestTime()));
        return false;
      } catch (IOException cause) {
        throw cause;
      } catch (Throwable t) {
        // For other type of exceptions, just throw an IOException. It will be handled by caller.
        throw new IOException(t);
      }
    }

    boolean processed = false;
    for (MessageAndOffset message : messages) {
      if (eventQueue.getEventSize() >= config.getMaxBufferSize()) {
        // Log a message. If this happen too often, it indicates that more memory is needed for the log processing
        LOG.info("Maximum queue size {} reached for pipeline {}.", config.getMaxBufferSize(), name);
        // If nothing has been appended (due to error), we break the loop so that no need event will be appended
        // Since the offset is not updated, the same set of messages will be fetched again in next iteration.
        int eventsAppended = appendEvents(System.currentTimeMillis(), true);
        if (eventsAppended <= 0) {
          break;
        }
        unSyncedEvents += eventsAppended;
      }

      try {
        ILoggingEvent loggingEvent = serializer.fromBytes(message.message().payload());
        // Use the message payload size as the size estimate of the logging event
        // Although it's not the same as the in memory object size, it should be just a constant factor, hence
        // it is proportional to the actual object size.
        eventQueue.add(loggingEvent, loggingEvent.getTimeStamp(),
                       message.message().payloadSize(), partition, message.nextOffset());
      } catch (IOException e) {
        // This should happen. In case it happens (e.g. someone published some garbage), just skip the message.
        LOG.warn("Fail to decode logging event from {}:{} at offset {}. Skipping it.",
                 topic, partition, message.offset(), e);
      }
      processed = true;
      offsets.put(partition, message.nextOffset());
    }

    return processed;
  }

  /**
   * Fetches messages from Kafka across all partitions simultaneously.
   */
  private <T extends Map<Integer, Future<Iterable<MessageAndOffset>>>> T fetchAll(Int2LongMap offsets,
                                                                                  T fetchFutures) {
    for (final int partition : config.getPartitions()) {
      final long offset = offsets.get(partition);

      fetchFutures.put(partition, fetchExecutor.submit(new Callable<Iterable<MessageAndOffset>>() {
        @Override
        public Iterable<MessageAndOffset> call() throws Exception {
          return fetchMessages(partition, offset);
        }
      }));
    }

    return fetchFutures;
  }

  /**
   * Appends buffered events to appender. If the {@code force} parameter is {@code false}, buffered events
   * that are older than the buffer milliseconds will be appended and removed from the buffer.
   * If {@code force} is {@code true}, then at least {@code maxQueueSize * MIN_FREE_FACTOR} events will be appended
   * and removed, regardless of the event time.
   *
   * @return number of events appended to the appender
   */
  private int appendEvents(long currentTimeMillis, boolean forced) {
    long minEventTime = currentTimeMillis - config.getEventDelayMillis();
    long maxRetainSize = forced ? (long) (config.getMaxBufferSize() * MIN_FREE_FACTOR) : Long.MAX_VALUE;

    TimeEventQueue.EventIterator<ILoggingEvent, Long> iterator = eventQueue.iterator();

    int eventsAppended = 0;
    while (iterator.hasNext()) {
      ILoggingEvent event = iterator.next();

      // If not forced to reduce the event queue size and the current event timestamp is still within the
      // buffering time, no need to iterate anymore
      if (eventQueue.getEventSize() <= maxRetainSize && event.getTimeStamp() >= minEventTime) {
        break;
      }

      try {
        // Otherwise, append the event
        ch.qos.logback.classic.Logger effectiveLogger = context.getEffectiveLogger(event.getLoggerName());
        if (event.getLevel().isGreaterOrEqual(effectiveLogger.getEffectiveLevel())) {
          effectiveLogger.callAppenders(event);
        }
      } catch (Exception e) {
        LOG.warn("Failed to append log event in pipeline {} due to {}. Will be retried.", name, e.getMessage());
        LOG.debug("Failed to append log event in pipeline {}.", name, e);
        break;
      }

      // Updates the Kafka offset before removing the current event
      int partition = iterator.getPartition();
      MutableCheckpoint checkpoint = checkpoints.get(partition);
      if (checkpoint == null) {
        checkpoint = new MutableCheckpoint(eventQueue.getSmallestOffset(partition), event.getTimeStamp());
        checkpoints.put(partition, checkpoint);
      } else {
        checkpoint
          .setNextOffset(eventQueue.getSmallestOffset(partition))
          .setMaxEventTime(event.getTimeStamp());
      }

      iterator.remove();
      eventsAppended++;
    }

    // For each partition, if there is no more event in the event queue, update the checkpoint nextOffset
    for (Int2LongMap.Entry entry : offsets.int2LongEntrySet()) {
      int partition = entry.getIntKey();
      if (eventQueue.isEmpty(partition)) {
        MutableCheckpoint checkpoint = checkpoints.get(partition);
        long offset = entry.getLongValue();
        // If the process offset is larger than the offset in the checkpoint and the queue is empty for that partition,
        // it means everything before the process offset must had been written to the appender.
        if (checkpoint != null && offset > checkpoint.getNextOffset()) {
          checkpoint.setNextOffset(offset);
        }
      }
    }

    // Always try to call flush, even there was no event written. This is needed so that appender get called
    // periodically even there is no new events being appended to perform housekeeping work.
    // Failure to flush is ok and it will be retried by the wrapped appender
    try {
      context.flush();
    } catch (IOException e) {
      LOG.warn("Failed to flush in pipeline {} due to {}. Will be retried.", name, e.getMessage());
      LOG.debug("Failed to flush in pipeline {}.", name, e);
    }

    return eventsAppended;
  }

  /**
   * Sync the appender and persists checkpoints if it is time.
   *
   * @return delay in millisecond till the next sync time.
   */
  private long trySyncAndPersistCheckpoints(long currentTimeMillis) {
    if (currentTimeMillis - config.getCheckpointIntervalMillis() < lastCheckpointTime || unSyncedEvents <= 0) {
      return config.getCheckpointIntervalMillis() - currentTimeMillis + lastCheckpointTime;
    }

    // Sync the appender and persists checkpoints
    try {
      context.sync();
      // Only persist if sync succeeded. Since persistCheckpoints never throw, it's ok to be inside the try.
      persistCheckpoints();
      lastCheckpointTime = currentTimeMillis;
      unSyncedEvents = 0;
      LOG.debug("Events synced and checkpoint persisted for {}", name);
    } catch (Exception e) {
      LOG.warn("Failed to sync in pipeline {} due to {}. Will be retried.", name, e.getMessage());
      LOG.debug("Failed to sync in pipeline {}.", name, e);
    }
    return config.getCheckpointIntervalMillis();
  }

  /**
   * Persists the checkpoints for all partitions.
   */
  private void persistCheckpoints() {
    try {
      checkpointManager.saveCheckpoints(checkpoints);
      LOG.debug("Checkpoint persisted for {} with {}", name, checkpoints);
    } catch (Exception e) {
      // Just log as it is non-fatal if failed to save checkpoints
      LOG.warn("Non-fatal failure when persist checkpoints for pipeline {}.", name, e);
    }
  }

  /**
   * Returns a {@link KafkaSimpleConsumer} for the given partition.
   */
  @Nullable
  private KafkaSimpleConsumer getKafkaConsumer(String topic, int partition) {
    BrokerInfo leader = brokerService.getLeader(topic, partition);
    if (leader == null) {
      return null;
    }

    KafkaSimpleConsumer consumer = kafkaConsumers.get(leader);
    if (consumer != null) {
      return consumer;
    }

    consumer = new KafkaSimpleConsumer(leader, KAFKA_SO_TIMEOUT, config.getKafkaFetchBufferSize(),
                                       "client-" + name + "-" + partition);
    kafkaConsumers.put(leader, consumer);
    return consumer;
  }

  /**
   * Fetch the latest Kafka offset published before the given timestamp. The timestamp can also be
   * special value {@link OffsetRequest$#EarliestTime()} or {@link OffsetRequest$#LatestTime()}.
   *
   * @param partition the partition for fetching the offset from.
   * @param timestamp the timestamp to use for fetching last offset before it
   * @return the latest offset
   * @throws IOException if there is error in fetching the offset.
   */
  private long getLastOffset(int partition, long timestamp) throws IOException {
    String topic = config.getTopic();
    KafkaSimpleConsumer consumer = getKafkaConsumer(topic, partition);
    if (consumer == null) {
      throw new IOException("No broker to fetch offsets for " + topic + ":" + partition);
    }

    // Fire offset request
    OffsetRequest request = new OffsetRequest(ImmutableMap.of(
      new TopicAndPartition(topic, partition),
      new PartitionOffsetRequestInfo(timestamp, 1)
    ), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    OffsetResponse response = consumer.getOffsetsBefore(request);

    // Retrieve offsets from response
    long[] offsets = response.hasError() ? null : response.offsets(topic, partition);
    if (offsets == null || offsets.length <= 0) {
      short errorCode = response.errorCode(topic, partition);

      // On error, clear the consumer cache
      kafkaConsumers.remove(consumer.getBrokerInfo());
      throw new IOException(String.format("Failed to fetch offset for %s:%s with timestamp %d. Error: %d.",
                                          topic, partition, timestamp, errorCode));
    }

    LOG.debug("Offset {} fetched for {}:{} with timestamp {}.", offsets[0], topic, partition, timestamp);
    return offsets[0];
  }

  /**
   * Fetch messages from Kafka.
   *
   * @param partition the partition to fetch from
   * @param offset the Kafka offset to fetch from
   * @return An {@link Iterable} of {@link MessageAndOffset}.
   *
   * @throws OffsetOutOfRangeException if the given offset is out of range.
   * @throws IOException if failed to fetch from Kafka.
   */
  private Iterable<MessageAndOffset> fetchMessages(int partition, long offset) throws IOException {
    String topic = config.getTopic();
    KafkaSimpleConsumer consumer = getKafkaConsumer(topic, partition);
    if (consumer == null) {
      throw new IOException("No broker to fetch messages for " + topic + ":" + partition);
    }

    LOG.trace("Fetching messages from Kafka on {}:{} for pipeline {} with offset {}", topic, partition, name, offset);

    FetchRequest request = new FetchRequestBuilder()
      .clientId(consumer.clientId())
      .addFetch(topic, partition, offset, config.getKafkaFetchBufferSize())
      .build();
    FetchResponse response = consumer.fetch(request);

    if (response.hasError()) {
      short errorCode = response.errorCode(topic, partition);

      if (errorCode == ErrorMapping.OffsetOutOfRangeCode()) {
        throw new OffsetOutOfRangeException(String.format("Kafka offset %d out of range for %s:%d",
                                                          offset, topic, partition));
      }

      // On all other errors, clear the consumer cache
      kafkaConsumers.remove(consumer.getBrokerInfo());
      throw new IOException(String.format("Failed to fetch message on %s:%d from %s",
                                          topic, partition, consumer.getBrokerInfo()));
    }

    ByteBufferMessageSet result = response.messageSet(topic, partition);
    LOG.trace("Fetched {} bytes from Kafka on {}:{} for pipeline {}", result.sizeInBytes(), topic, partition, name);

    return result;
  }

  /**
   * A {@link SimpleConsumer} that allows getting back the {@link BrokerInfo} used to create the consumer.
   */
  private static final class KafkaSimpleConsumer extends SimpleConsumer {

    private final BrokerInfo brokerInfo;

    KafkaSimpleConsumer(BrokerInfo brokerInfo, int soTimeout, int bufferSize, String clientId) {
      super(brokerInfo.getHost(), brokerInfo.getPort(), soTimeout, bufferSize, clientId);
      this.brokerInfo = brokerInfo;
    }

    BrokerInfo getBrokerInfo() {
      return brokerInfo;
    }
  }

  /**
   * A mutable implementation of {@link Checkpoint}.
   */
  private static final class MutableCheckpoint extends Checkpoint {

    private long nextOffset;
    private long maxEventTime;

    MutableCheckpoint(Checkpoint other) {
      this(other.getNextOffset(), other.getMaxEventTime());
    }

    MutableCheckpoint(long nextOffset, long maxEventTime) {
      super(nextOffset, maxEventTime);
      this.nextOffset = nextOffset;
      this.maxEventTime = maxEventTime;
    }

    @Override
    public long getNextOffset() {
      return nextOffset;
    }

    MutableCheckpoint setNextOffset(long nextOffset) {
      this.nextOffset = nextOffset;
      return this;
    }

    @Override
    public long getMaxEventTime() {
      return maxEventTime;
    }

    MutableCheckpoint setMaxEventTime(long maxEventTime) {
      this.maxEventTime = maxEventTime;
      return this;
    }

    @Override
    public String toString() {
      return "Checkpoint{" +
        "nextOffset=" + nextOffset +
        ", maxEventTime=" + maxEventTime +
        '}';
    }
  }
}
