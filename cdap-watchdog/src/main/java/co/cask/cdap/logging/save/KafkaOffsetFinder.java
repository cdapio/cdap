/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSEEARLIEST.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.logging.save;

import ch.qos.logback.classic.spi.ILoggingEvent;
import co.cask.cdap.common.utils.Tasks;
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import com.google.common.collect.ImmutableMap;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import org.apache.twill.common.Cancellable;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Finds matching Kafka offset with given checkpoint.
 */
public class KafkaOffsetFinder {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetFinder.class);

  private static final int FETCH_SIZE = 1024 * 1024;        // Use a default fetch size.
  private static final int SO_TIMEOUT = 5 * 1000;           // 5 seconds.
  private static final long LATEST = kafka.api.OffsetRequest.LatestTime();
  private static final long EARLIEST = kafka.api.OffsetRequest.EarliestTime();

  private final BrokerService brokerService;
  private final KafkaConsumer consumer;
  private final String topic;
  private final LoggingEventSerializer serializer;

  public KafkaOffsetFinder(BrokerService brokerService, KafkaConsumer consumer, String topic) throws IOException {
    this.brokerService = brokerService;
    this.consumer = consumer;
    this.topic = topic;
    this.serializer = new LoggingEventSerializer();
  }

  public long getMatchingOffset(final Checkpoint checkpoint, final int partition) {
    if (checkpoint.getNextOffset() < 0) {
      return checkpoint.getNextOffset();
    }

    // Get BrokerInfo for constructing SimpleConsumer in OffsetFinderCallback
    BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
    if (brokerInfo == null) {
      return EARLIEST; // Return earliest offset if brokerInfo is null
    }
    final BlockingQueue<Long> offsetQueue = new LinkedBlockingQueue<>();
    brokerService.getLeader(topic, partition);
    SimpleConsumer simpleConsumer
      = new SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                                                  SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");
    long minOffset = getBoundaryOffset(simpleConsumer, partition, EARLIEST);
    // Latest offset returned is 1 larger than the latest received message's offset
    long maxOffset = getBoundaryOffset(simpleConsumer, partition, LATEST) - 1;
    OffsetFinderCallback callback =
      new OffsetFinderCallback(offsetQueue, brokerInfo, simpleConsumer, checkpoint.getMaxEventTime(), topic, partition,
                               minOffset, maxOffset);
    long startOffset =
      (checkpoint.getNextOffset() > callback.getMaxOffset() || checkpoint.getNextOffset() < callback.getMinOffset()) ?
      EARLIEST : checkpoint.getNextOffset();
    Cancellable findCancel = consumer.prepare().add(topic, partition, startOffset)
      .consume(callback);
    Long targetOffset;
    try {
      // Wait for the offset found by OffsetFinderCallback to be put in the offsetQueue
      targetOffset = offsetQueue.poll(3, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Interrupted when waiting for the matching offset from Kafka.", e);
      findCancel.cancel();
      return EARLIEST;
    }
    if (targetOffset == null) {
      targetOffset = getBoundaryOffset(simpleConsumer, partition, LATEST);
      LOG.debug("Failed to find matching offset from Kafka because the timestamp in stored checkpoint {} " +
                  "is larger than all timestamps of the received messages. Return the latest offset {}",
                checkpoint, targetOffset);
    }
    findCancel.cancel();
    return targetOffset;
  }

  private class OffsetFinderCallback implements KafkaConsumer.MessageCallback {

    private final BlockingQueue<Long> offsetQueue;
    private final long targetTimestamp;
    private final String topic;
    private final int partition;
    private final BrokerInfo brokerInfo;

    private long minOffset;
    private long maxOffset;
    private SimpleConsumer simpleConsumer;

    OffsetFinderCallback(final BlockingQueue<Long> offsetQueue, BrokerInfo brokerInfo,
                         SimpleConsumer simpleConsumer, long targetTimestamp,
                         String topic, int partition, long minOffset, long maxOffset) {
      this.offsetQueue = offsetQueue;
      this.targetTimestamp = targetTimestamp;
      this.topic = topic;
      this.partition = partition;
      this.brokerInfo = brokerInfo;
      this.simpleConsumer = simpleConsumer;
      this.minOffset = minOffset;
      this.maxOffset = maxOffset;
    }

    @Override
    // Use binary search to find the offset of the message with the timestamp matching targetTimestamp.
    // Return the offset to fetch the next message until the matching message is found.
    public long onReceived(Iterator<FetchedMessage> messages) {
      FetchedMessage message = messages.next();
      long offset = message.getOffset();
      ILoggingEvent event;
      try {
        event = serializer.fromBytes(message.getPayload());
      } catch (IOException e) {
        LOG.warn("Message with offset {} in topic {} partition {} is ignored because of failure to decode.",
                 offset, topic, partition, e);
        return message.getNextOffset();
      }
      LOG.trace("Received event {} for topic {} partition {}", event, topic, partition);
      long timeStamp = event.getTimeStamp();
      if (timeStamp == targetTimestamp) {
        if (offsetQueue.isEmpty()) {
          offsetQueue.offer(offset);
        }
        return offset;
      }

      if (timeStamp > targetTimestamp) {
        // targetTimestamp is smaller than all existing messages's timestamp
        if (offset - 1 < minOffset) {
          if (offsetQueue.isEmpty()) {
            offsetQueue.offer(minOffset);
          }
          return minOffset;
        }
        maxOffset = offset - 1;
        long newOffset = minOffset + (maxOffset - minOffset) / 2;
        LOG.debug("timeStamp > targetTimestamp, return newOffset {}", newOffset);
        return newOffset;
      }
      if (timeStamp < targetTimestamp) {
        // targetTimestamp is larger than all existing messages's timestamp, wait for new messages to come
        if (offset + 1 > maxOffset) {
          maxOffset = getBoundaryOffset(simpleConsumer, partition, LATEST) - 1;
          return maxOffset;
        }
        minOffset = offset + 1;
        long newOffset = minOffset + (maxOffset - minOffset) / 2;

        LOG.debug("timeStamp < targetTimestamp, return newOffset {}", newOffset);
        return newOffset;
      }
      // Should never reach here
      return LATEST;
    }

    @Override
    public void finished() {
      //no-op
    }

    long getMinOffset() {
      return minOffset;
    }

    long getMaxOffset() {
      return maxOffset;
    }
  }

  /**
   * Gets earliest or latest message's offset
   */
  private long getBoundaryOffset(SimpleConsumer simpleConsumer, int partition, long timeStamp) {
    // If no broker, treat it as failure attempt.
    if (simpleConsumer == null) {
      LOG.error("Failed to talk to any broker. Default offset to 0 for {}-{}", topic, partition);
      return 0L;
    }
    // Fire offset request
    kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(ImmutableMap.of(
      new TopicAndPartition(topic, partition),
      new PartitionOffsetRequestInfo(timeStamp, 1)
    ), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());

    OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

    // Retrieve offsets from response
    long[] offsets = response.hasError() ? null : response.offsets(topic, partition);
    if (offsets == null || offsets.length <= 0) {
      short errorCode = response.errorCode(topic, partition);

      // If the topic partition doesn't exists, use offset 0 without logging error.
      if (errorCode != ErrorMapping.UnknownTopicOrPartitionCode()) {
        LOG.error("Failed to fetch offset for {}-{} with timestamp {}. Error: {}. Default offset to 0.",
                 topic, partition, timeStamp, errorCode);
      }
      return 0L;
    }

    return offsets[0];
  }
}
