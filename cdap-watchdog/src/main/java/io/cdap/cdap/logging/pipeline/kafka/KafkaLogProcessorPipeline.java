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
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.common.logging.LogSamplers;
import co.cask.cdap.common.logging.Loggers;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.meta.CheckpointManager;
import co.cask.cdap.logging.meta.KafkaOffset;
import co.cask.cdap.logging.pipeline.LogProcessorPipelineContext;
import co.cask.cdap.logging.pipeline.queue.ProcessedEventMetadata;
import co.cask.cdap.logging.pipeline.queue.ProcessorEvent;
import co.cask.cdap.logging.pipeline.queue.TimeEventQueueProcessor;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.AbstractExecutionThreadService;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import kafka.api.OffsetRequest$;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.twill.common.Threads;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
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
  // For outage, only log once per 60 seconds per message.
  private static final Logger OUTAGE_LOG =
    Loggers.sampling(LOG, LogSamplers.perMessage(() -> LogSamplers.limitRate(60000)));

  private static final int KAFKA_SO_TIMEOUT = 3000;

  private final String name;
  private final LogProcessorPipelineContext context;
  private final CheckpointManager<KafkaOffset> checkpointManager;
  private final Int2LongMap offsets;
  private final Int2ObjectMap<MutableCheckpoint> checkpoints;
  private final LoggingEventSerializer serializer;
  private final KafkaPipelineConfig config;
  private final TimeEventQueueProcessor<KafkaOffset> eventQueueProcessor;
  private final MetricsContext metricsContext;

  private final BrokerService brokerService;
  private final KafkaOffsetResolver offsetResolver;
  private final Map<BrokerInfo, KafkaSimpleConsumer> kafkaConsumers;

  private ExecutorService fetchExecutor;
  private volatile Thread runThread;
  private volatile boolean stopped;
  private long lastCheckpointTime;
  private int unSyncedEvents;

  public KafkaLogProcessorPipeline(LogProcessorPipelineContext context,
                                   CheckpointManager<KafkaOffset> checkpointManager, BrokerService brokerService,
                                   KafkaPipelineConfig config) {
    this.name = context.getName();
    this.context = context;
    this.checkpointManager = checkpointManager;
    this.brokerService = brokerService;
    this.config = config;
    this.offsets = new Int2LongOpenHashMap();
    this.checkpoints = new Int2ObjectOpenHashMap<>();
    this.eventQueueProcessor = new TimeEventQueueProcessor<>(context, config.getMaxBufferSize(),
                                                             config.getEventDelayMillis(), config.getPartitions());
    this.serializer = new LoggingEventSerializer();
    this.metricsContext = context;
    this.kafkaConsumers = new HashMap<>();
    this.offsetResolver = new KafkaOffsetResolver(brokerService, config);
  }

  @Override
  protected void startUp() throws Exception {
    LOG.debug("Starting log processor pipeline for {} with configurations {}", name, config);

    // Reads the existing checkpoints
    Set<Integer> partitions = config.getPartitions();
    for (Map.Entry<Integer, Checkpoint<KafkaOffset>> entry : checkpointManager.getCheckpoint(partitions).entrySet()) {
      Checkpoint<KafkaOffset> checkpoint = entry.getValue();
      KafkaOffset kafkaOffset = checkpoint.getOffset();
      // Skip the partition that doesn't have previous checkpoint.
      if (kafkaOffset.getNextOffset() >= 0 && kafkaOffset.getNextEventTime() >= 0 &&
        checkpoint.getMaxEventTime() >= 0) {
        checkpoints.put(entry.getKey(), new MutableCheckpoint(checkpoint));
      }
    }

    context.start();

    fetchExecutor = Executors.newFixedThreadPool(
      partitions.size(), Threads.createDaemonThreadFactory("fetcher-" + name + "-%d"));

    // emit pipeline related config as metrics
    emitConfigMetrics();

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
          } catch (IOException | KafkaException e) {
            OUTAGE_LOG.warn("Failed to fetch or process messages from {}:{}. Will be retried in next iteration.",
                            topic, partition, e);
          }
        }

        long now = System.currentTimeMillis();
        long nextCheckpointDelay = trySyncAndPersistCheckpoints(now);

        // If nothing has been processed (e.g. fail to append anything to appender),
        // Sleep until the earliest event in the buffer is time to be written out.
        if (!hasMessageProcessed) {
          long sleepMillis = Math.min(config.getEventDelayMillis(), nextCheckpointDelay);
          if (sleepMillis > 0) {
            TimeUnit.MILLISECONDS.sleep(sleepMillis);
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
    Set<Integer> partitions = new HashSet<>(config.getPartitions());

    while (!partitions.isEmpty() && !stopped) {
      Iterator<Integer> iterator = partitions.iterator();
      boolean failed = false;
      while (iterator.hasNext()) {
        int partition = iterator.next();
        MutableCheckpoint checkpoint = checkpoints.get(partition);
        try {
          if (checkpoint == null || checkpoint.getOffset().nextOffset <= 0) {
            // If no checkpoint, fetch from the beginning.
            offsets.put(partition, getLastOffset(partition, kafka.api.OffsetRequest.EarliestTime()));
          } else {
            // Otherwise, validate and find the offset if not valid
            MutableKafkaOffset kafkaOffset = checkpoint.getOffset();
            offsets.put(partition, offsetResolver.getStartOffset(kafkaOffset.nextOffset, kafkaOffset.nextEventTime,
                                                                 partition));
          }
          // Remove the partition successfully stored in offsets to avoid unnecessary retry for this partition
          iterator.remove();
        } catch (Exception e) {
          OUTAGE_LOG.warn("Failed to get a valid offset from Kafka to start consumption for {}:{}",
                          config.getTopic(), partition);
          failed = true;
        }
      }

      // Should keep finding valid offsets for all partitions
      if (failed && !stopped) {
        TimeUnit.SECONDS.sleep(2L);
      }
    }
  }

  /**
   * Processes log messages for given partition. This method will provide events iterator to event queue processor
   * and update checkpoints and offsets based on events processed by event queue processor. If any of the events are
   * processed by event queue processor, method returns true, otherwise false.
   */
  private boolean processMessages(String topic, int partition,
                                  Future<Iterable<MessageAndOffset>> future) throws InterruptedException,
                                                                                    KafkaException, IOException {
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
      } catch (KafkaException | IOException cause) {
        throw cause;
      } catch (Throwable t) {
        // For other type of exceptions, just throw an IOException. It will be handled by caller.
        throw new IOException(t);
      }
    }

    // process all the messages
    ProcessedEventMetadata<KafkaOffset> metadata = eventQueueProcessor
      .process(partition, new KafkaMessageTransformIterator(topic, partition, messages.iterator()));

    // None of the events were processed.
    if (metadata.getTotalEventsProcessed() <= 0) {
      return false;
    }

    // only update checkpoints if some events were processed
    unSyncedEvents += metadata.getTotalEventsProcessed();

    for (Map.Entry<Integer, Checkpoint<KafkaOffset>> entry : metadata.getCheckpoints().entrySet()) {
      MutableCheckpoint checkpoint = checkpoints.get(entry.getKey());
      Checkpoint<KafkaOffset> checkpointMetadata = entry.getValue();
      // Update checkpoints
      KafkaOffset kafkaOffset = checkpointMetadata.getOffset();
      if (checkpoint == null) {
        checkpoint = new MutableCheckpoint(kafkaOffset, checkpointMetadata.getMaxEventTime());
        checkpoints.put(entry.getKey(), checkpoint);
      } else {
        MutableKafkaOffset offset = checkpoint.getOffset();
        offset.setNextOffset(kafkaOffset.getNextOffset());
        offset.setNextEventTime(kafkaOffset.getNextEventTime());
        checkpoint.setMaxEventTime(checkpointMetadata.getMaxEventTime());
      }
    }

    // For each partition, if there is no more event in the event queue, update the checkpoint nextOffset
    for (Int2LongMap.Entry entry : offsets.int2LongEntrySet()) {
      if (eventQueueProcessor.isQueueEmpty(entry.getIntKey())) {
        MutableCheckpoint checkpoint = checkpoints.get(entry.getIntKey());
        long offset = entry.getLongValue();
        // If the process offset is larger than the offset in the checkpoint and the queue is empty for that partition,
        // it means everything before the process offset must had been written to the appender.
        if (checkpoint != null && offset > checkpoint.getOffset().getNextOffset()) {
          checkpoint.getOffset().setNextOffset(offset);
        }
      }
    }

    return true;
  }

  /**
   * Fetches messages from Kafka across all partitions simultaneously.
   */
  private <T extends Map<Integer, Future<Iterable<MessageAndOffset>>>> T fetchAll(Int2LongMap offsets,
                                                                                  T fetchFutures) {
    for (final int partition : config.getPartitions()) {
      final long offset = offsets.get(partition);

      fetchFutures.put(partition, fetchExecutor.submit(() -> fetchMessages(partition, offset)));
    }

    return fetchFutures;
  }

  /**
   * Sync the appender and persists checkpoints if it is time.
   *
   * @return delay in millisecond till the next sync time.
   */
  private long trySyncAndPersistCheckpoints(long currentTimeMillis) {
    if (unSyncedEvents <= 0) {
      return config.getCheckpointIntervalMillis();
    }
    if (currentTimeMillis - config.getCheckpointIntervalMillis() < lastCheckpointTime) {
      return lastCheckpointTime + config.getCheckpointIntervalMillis() - currentTimeMillis;
    }

    // Sync the appender and persists checkpoints
    try {
      context.sync();
      // Only persist if sync succeeded. Since persistCheckpoints never throw, it's ok to be inside the try.
      persistCheckpoints();
      lastCheckpointTime = currentTimeMillis;
      metricsContext.gauge("last.checkpoint.time", lastCheckpointTime);
      unSyncedEvents = 0;
      LOG.debug("Events synced and checkpoint persisted for {}", name);
    } catch (Exception e) {
      OUTAGE_LOG.warn("Failed to sync in pipeline {}. Will be retried.", name, e);
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
    } catch (IOException e) {
      // Just log as it is non-fatal if failed to save checkpoints
      OUTAGE_LOG.warn("Failed to persist checkpoints for pipeline {}.", name, e);
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
   *
   * @throws LeaderNotAvailableException if no leading broker is available for the given partition
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  private long getLastOffset(int partition, long timestamp) throws KafkaException {
    String topic = config.getTopic();
    KafkaSimpleConsumer consumer = getKafkaConsumer(topic, partition);
    if (consumer == null) {
      throw new LeaderNotAvailableException("No broker to fetch offsets for " + topic + ":" + partition);
    }
    try {
      return KafkaUtil.getOffsetByTimestamp(consumer, topic, partition, timestamp);
    } catch (KafkaException e) {
      // On error, clear the consumer cache
      kafkaConsumers.remove(consumer.getBrokerInfo());
      throw e;
    }
  }

  /**
   * Fetch messages from Kafka.
   *
   * @param partition the partition to fetch from
   * @param offset the Kafka offset to fetch from
   * @return An {@link Iterable} of {@link MessageAndOffset}.
   *
   * @throws LeaderNotAvailableException if there is no Kafka broker to talk to.
   * @throws OffsetOutOfRangeException if the given offset is out of range.
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  private Iterable<MessageAndOffset> fetchMessages(int partition, long offset) throws KafkaException {
    String topic = config.getTopic();
    KafkaSimpleConsumer consumer = getKafkaConsumer(topic, partition);
    if (consumer == null) {
      throw new LeaderNotAvailableException("No broker to fetch messages for " + topic + ":" + partition);

    }

    LOG.trace("Fetching messages from Kafka on {}:{} for pipeline {} with offset {}", topic, partition, name, offset);
    try {
      ByteBufferMessageSet result = KafkaUtil.fetchMessages(consumer, topic, partition,
                                                            config.getKafkaFetchBufferSize(), offset);
      LOG.trace("Fetched {} bytes from Kafka on {}:{} for pipeline {}", result.sizeInBytes(), topic, partition, name);

      return result;
    } catch (OffsetOutOfRangeException e) {
      // If the error is not offset out of range, clear the consumer cache
      kafkaConsumers.remove(consumer.getBrokerInfo());
      throw e;
    }
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

  private void emitConfigMetrics() {
    metricsContext.gauge("max.buffer.size", config.getMaxBufferSize());
    metricsContext.gauge("event.delay.millis", config.getEventDelayMillis());
    metricsContext.gauge("kafka.fetch.buffer.size", config.getKafkaFetchBufferSize());
    metricsContext.gauge("checkpoint.interval.millis", config.getCheckpointIntervalMillis());
  }

  /**
   * A mutable implementation of {@link Checkpoint}.
   */
  private static final class MutableCheckpoint extends Checkpoint<KafkaOffset> {

    private MutableKafkaOffset offset;
    private long maxEventTime;

    MutableCheckpoint(Checkpoint<KafkaOffset> other) {
      this(other.getOffset(), other.getMaxEventTime());
      this.offset = new MutableKafkaOffset(other.getOffset().getNextOffset(), other.getOffset().getNextEventTime());
      this.maxEventTime = other.getMaxEventTime();
    }

    MutableCheckpoint(KafkaOffset offset, long maxEventTime) {
      super(offset, maxEventTime);
      this.offset = new MutableKafkaOffset(offset);
      this.maxEventTime = maxEventTime;
    }

    @Override
    public MutableKafkaOffset getOffset() {
      return offset;
    }

    void setOffset(MutableKafkaOffset offset) {
      this.offset = offset;
    }

    @Override
    public long getMaxEventTime() {
      return maxEventTime;
    }

    void setMaxEventTime(long maxEventTime) {
      this.maxEventTime = maxEventTime;
    }

    @Override
    public String toString() {
      return "MutableCheckpoint{" +
        "offset=" + offset +
        ", maxEventTime=" + maxEventTime +
        '}';
    }
  }

  /**
   * A mutable implementation of {@link KafkaOffset}.
   */
  private static final class MutableKafkaOffset extends KafkaOffset {
    private long nextOffset;
    private long nextEventTime;

    MutableKafkaOffset(KafkaOffset offset) {
      this(offset.getNextOffset(), offset.getNextEventTime());
      this.nextOffset = offset.getNextOffset();
      this.nextEventTime = offset.getNextEventTime();
    }

    MutableKafkaOffset(long nextOffset, long nextEventTime) {
      super(nextOffset, nextEventTime);
      this.nextOffset = nextOffset;
      this.nextEventTime = nextEventTime;
    }

    @Override
    public long getNextOffset() {
      return nextOffset;
    }

    void setNextOffset(long nextOffset) {
      this.nextOffset = nextOffset;
    }

    @Override
    public long getNextEventTime() {
      return nextEventTime;
    }

    void setNextEventTime(long nextEventTime) {
      this.nextEventTime = nextEventTime;
    }

    @Override
    public String toString() {
      return "MutableKafkaOffset{" +
        "nextOffset=" + nextOffset +
        ", nextEventTime=" + nextEventTime +
        '}';
    }
  }

  /**
   * Transforms kafka {@code MessageAndOffset} iterator to {@code ProcessorEvent} iterator.
   */
  private final class KafkaMessageTransformIterator extends AbstractIterator<ProcessorEvent<KafkaOffset>> {
    private final String topic;
    private final int partition;
    private final Iterator<MessageAndOffset> kafkaMessageIterator;
    private Long prevOffset;

    KafkaMessageTransformIterator(String topic, int partition, Iterator<MessageAndOffset> kafkaMessageIterator) {
      this.topic = topic;
      this.partition = partition;
      this.kafkaMessageIterator = kafkaMessageIterator;
    }

    @Override
    protected ProcessorEvent<KafkaOffset> computeNext() {
      if (!kafkaMessageIterator.hasNext()) {
        updateOffsets();
        return endOfData();
      }

      boolean skipped;
      ProcessorEvent<KafkaOffset> nextEntry = null;

      do {
        updateOffsets();
        MessageAndOffset message = kafkaMessageIterator.next();
        metricsContext.increment("kafka.bytes.read", message.message().payloadSize());

        try {
          ILoggingEvent loggingEvent = serializer.fromBytes(message.message().payload());
          nextEntry = new ProcessorEvent<>(loggingEvent, message.message().payloadSize(),
                                           new KafkaOffset(message.nextOffset(), loggingEvent.getTimeStamp()));
          skipped = false;
        } catch (IOException e) {
          skipped = true;
          // This shouldn't happen. In case it happens (e.g. someone published some garbage), just skip the message.
          LOG.trace("Fail to decode logging event from {}:{} at offset {}. Skipping it.",
                    topic, partition, message.offset(), e);
        }
      } while (skipped && kafkaMessageIterator.hasNext());

      if (nextEntry == null) {
        updateOffsets();
        return endOfData();
      }

      prevOffset = nextEntry.getOffset().getNextOffset();
      return nextEntry;
    }

    private void updateOffsets() {
      if (prevOffset != null) {
        offsets.put(partition, prevOffset.longValue());
      }
    }
  }
}
