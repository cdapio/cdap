package com.continuuity.logging.tail;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.continuuity.api.data.OperationException;
import com.continuuity.app.logging.FlowletLoggingContext;
import com.continuuity.common.logging.logback.kafka.LoggingEventSerializer;
import com.continuuity.common.utils.ImmutablePair;
import com.continuuity.logging.kafka.KafkaConsumer;
import com.continuuity.logging.kafka.KafkaMessage;
import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Tails logs by reading the log messages from Kafka.
 */
public class LogTail {
  private static final Logger LOG = LoggerFactory.getLogger(LogTail.class);

  private static final int MAX_RETURN_COUNT = 1000;
  private static final int WINDOW_SIZE = 1000;
  private List<ImmutablePair<String, Integer>> seedBrokers;
  private String topic;
  private int numPartitions;
  private LoggingEventSerializer serializer;

  private final int kafkaTailFetchTimeoutMs = 300;

  /**
   * Creates a LogTail object.
   * @param seedBrokers initial list of Kafka brokers.
   * @param topic topic to subscribe to.
   * @param numPartitions total number of Kafka partitions available.
   * @param serializer the serializer to use for de-serializing message into log event.
   */
  public LogTail(List<ImmutablePair<String, Integer>> seedBrokers, String topic, int numPartitions,
                 LoggingEventSerializer serializer) {
    this.seedBrokers = seedBrokers;
    this.topic = topic;
    this.numPartitions = numPartitions;
    this.serializer = serializer;
  }

  /**
   * Method to tail the log of a Flow.
   * @param accountId account Id of the Flow.
   * @param applicationId application Id of the Flow.
   * @param flowId Id of the Flow.
   * @param fromTimeMs time in ms from epoch to start the tail from.
   * @return list of log events.
   * @throws OperationException
   */
  public List<ILoggingEvent> tailFlowLog(String accountId, String applicationId,
                                         String flowId, long fromTimeMs) throws OperationException {
    FlowletLoggingContext flowletLoggingContext = new FlowletLoggingContext(accountId, applicationId, flowId, "");
    return tailLog(flowletLoggingContext.getLogPartition().hashCode() % numPartitions, fromTimeMs);
  }

  /**
   * Internal method to tail log events from a partition.
   * @param partition partition to read.
   * @param fromTimeMs time ins ms from epoch to start the tail from.
   * @return list of log events.
   * @throws OperationException
   */
  List<ILoggingEvent> tailLog(int partition, long fromTimeMs) throws OperationException {
    KafkaConsumer kafkaConsumer = new KafkaConsumer(seedBrokers, topic, partition, kafkaTailFetchTimeoutMs);
    List<ILoggingEvent> loggingEvents = Lists.newArrayListWithExpectedSize(MAX_RETURN_COUNT);
    List<KafkaMessage> kafkaMessages = Lists.newArrayListWithExpectedSize(MAX_RETURN_COUNT);

    try {
      final long earliestOffset = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.EARLIEST);
      long endWindow = kafkaConsumer.fetchOffset(KafkaConsumer.Offset.LATEST);

      while (loggingEvents.size() <= MAX_RETURN_COUNT) {
        long beginWindow = endWindow - WINDOW_SIZE;
        if (beginWindow < earliestOffset) {
          beginWindow = earliestOffset;
        }

        if (beginWindow >= endWindow) {
          break;
        }

        kafkaMessages.clear();
        kafkaConsumer.fetchMessages(beginWindow, kafkaMessages);
        if (kafkaMessages.isEmpty()) {
          break;
        }

        endWindow = kafkaMessages.get(0).getOffset() - 1;
        for (KafkaMessage message : kafkaMessages) {
          ILoggingEvent event = serializer.fromBytes(message.getByteBuffer().array());
          if (event.getTimeStamp() >= fromTimeMs) {
            loggingEvents.add(event);
          } else {
            break;
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

    Collections.sort(loggingEvents, LOGGING_EVENT_COMPARATOR);
    return loggingEvents;
  }

  private static final Comparator<ILoggingEvent> LOGGING_EVENT_COMPARATOR =
    new Comparator<ILoggingEvent>() {
      @Override
      public int compare(ILoggingEvent e1, ILoggingEvent e2) {
        return e1.getTimeStamp() == e2.getTimeStamp() ? 0 : (e1.getTimeStamp() > e2.getTimeStamp() ? 1 : -1);
      }
    };
}
