package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.common.conf.CConfiguration;
import com.continuuity.common.logging.LoggingConfiguration;
import com.continuuity.common.logging.logback.kafka.KafkaTopic;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import com.continuuity.logging.filter.Filter;
import com.continuuity.logging.filter.LogFilterGenerator;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.kafka.KafkaMessage;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Tails logs by reading the log messages from Kafka.
 */
public final class LogTail {
  private static final Logger LOG = LoggerFactory.getLogger(LogTail.class);

  private static final int MAX_RETURN_COUNT = 1000;
  private static final int WINDOW_SIZE = 1000;
  private final List<LoggingConfiguration.KafkaHost> seedBrokers;
  private final String topic;
  private final int numPartitions;
  private final LoggingEventSerializer serializer;

  private final int kafkaTailFetchTimeoutMs = 300;

  /**
   * Creates a LogTail object.
   * @param configuration configuration object containing Kafka seed brokers and number of Kafka partitions for log
   *                      topic.
   */
  public LogTail(CConfiguration configuration) {
    try {
      this.seedBrokers = LoggingConfiguration.getKafkaSeedBrokers(
        configuration.get(LoggingConfiguration.KAFKA_SEED_BROKERS));
      Preconditions.checkArgument(!this.seedBrokers.isEmpty(), "Kafka seed brokers list is empty!");

      this.topic = KafkaTopic.getTopic();
      Preconditions.checkArgument(!this.topic.isEmpty(), "Kafka topic is emtpty!");

      this.numPartitions = configuration.getInt(LoggingConfiguration.NUM_PARTITONS, -1);
      Preconditions.checkArgument(this.numPartitions > 0,
                                  "numPartitions should be greater than 0. Got numPartitions=%s", this.numPartitions);

      this.serializer = new LoggingEventSerializer();
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  /**
   * Method to tail the log of a Flow.
   * @param accountId account Id of the Flow.
   * @param applicationId application Id of the Flow.
   * @param flowId Id of the Flow.
   * @param sizeBytes max size of log events to return.
   * @param callback Callback interface to receive logging event.
   * @throws OperationException
   */
  public void tailFlowLog(String accountId, String applicationId, String flowId, int sizeBytes, Callback callback)
    throws OperationException {
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext(accountId, applicationId, flowId, "");
    Filter logFilter = LogFilterGenerator.createTailFilter(accountId, applicationId, flowId);
    tailLog(flowletLoggingContext.getLogPartition().hashCode() % numPartitions, sizeBytes, logFilter, callback);
  }

  /**
   * Internal method to tail log events from a partition.
   * @param partition partition to read.
   * @param sizeBytes max size of log events to return.
   * @param callback Callback interface to receive logging event.
   * @throws OperationException
   */
  private void tailLog(int partition, int sizeBytes, Filter logFilter,
                       Callback callback) throws OperationException {
    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);
    List<KafkaMessage> kafkaMessages = Lists.newArrayListWithExpectedSize(MAX_RETURN_COUNT);

    try {
      final long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
      long endWindow = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);
      int fetchSizeBytes = sizeBytes;

      while (fetchSizeBytes > 0) {
        long beginWindow = endWindow - WINDOW_SIZE;
        if (beginWindow < earliestOffset) {
          beginWindow = earliestOffset;
        }

        if (beginWindow >= endWindow) {
          break;
        }

        kafkaMessages.clear();
        int fetchedBytes = kafkaConsumer.fetchMessages(beginWindow, fetchSizeBytes, kafkaMessages);
        fetchSizeBytes -= fetchedBytes;
        if (kafkaMessages.isEmpty()) {
          break;
        }

        endWindow = kafkaMessages.get(0).getOffset() - 1;
        for (KafkaMessage message : kafkaMessages) {
          ILoggingEvent event = serializer.fromBytes(message.getByteBuffer().array());
          if (logFilter.match(event)) {
            callback.handle(event);
          }
        }
      }
    } finally {
      try {
        kafkaConsumer.close();
      } catch (IOException e) {
        LOG.error(String.format("Caught exception when closing KafkaConsumer for topic %s, partition %d",
                                topic, partition), e);
      }
    }
  }
}
