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
  private static final int TIME_MATCH = 1;
  private static final int TIME_NOT_MATCH = -1;

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

    // First try to check whether the message with the offset in the given checkpoint has the same timestamp.
    // If the message has the same timestamp as in the checkpoint, directly return the checkpoint's offset.
    final AtomicInteger matchFound = new AtomicInteger(0);
    Cancellable initCancel = null;
    try {
      initCancel = consumer.prepare().add(topic, partition, checkpoint.getNextOffset())
        .consume(new KafkaConsumer.MessageCallback() {
          @Override
          public long onReceived(Iterator<FetchedMessage> messages) {
            if (!messages.hasNext()) {
              return EARLIEST;
            }
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
            long timestamp = event.getTimeStamp();
            if (offset == checkpoint.getNextOffset()) {
              matchFound.set(timestamp == checkpoint.getMaxEventTime() ? TIME_MATCH : TIME_NOT_MATCH);
            }
            return message.getNextOffset();
          }

          @Override
          public void finished() {
            // no-op
          }
        });
      Tasks.waitFor(true, new Callable<Boolean>() {
        @Override
        public Boolean call() throws Exception {
          return matchFound.get() != 0;
        }
      }, 1500, TimeUnit.MILLISECONDS);

      if (matchFound.get() == TIME_MATCH) {
        initCancel.cancel();
        return checkpoint.getNextOffset();
      }

    } catch (Exception e) {
      LOG.trace("Given checkpoint {} has mismatching offset and timestamp. Continue to search for the correct offset" +
                  "with the given timestamp.", checkpoint, e);
    }
    if (initCancel != null) {
      initCancel.cancel();
    }

    // Get BrokerInfo for constructing kafka.javaapi.consumer.SimpleConsumer in OffsetFinderCallback
    BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
    if (brokerInfo == null) {
      return EARLIEST; // Return earliest offset if brokerInfo is null
    }
    final BlockingQueue<Long> offsetQueue = new LinkedBlockingQueue<>();
    OffsetFinderCallback callback =
      new OffsetFinderCallback(offsetQueue, brokerInfo, checkpoint.getMaxEventTime(), topic, partition);
    Cancellable findCancel = consumer.prepare().add(topic, partition, EARLIEST)
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
      targetOffset = callback.getBoundaryOffset(LATEST);
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
    private kafka.javaapi.consumer.SimpleConsumer simpleConsumer;

    OffsetFinderCallback(final BlockingQueue<Long> offsetQueue, BrokerInfo brokerInfo, long targetTimestamp,
                         String topic, int partition) {
      this.offsetQueue = offsetQueue;
      this.targetTimestamp = targetTimestamp;
      this.topic = topic;
      this.partition = partition;
      this.brokerInfo = brokerService.getLeader(topic, partition);
      this.simpleConsumer
        = new kafka.javaapi.consumer.SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                                                    SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");
      this.minOffset = getBoundaryOffset(EARLIEST);
      // Latest offset returned is 1 larger than the latest received message's offset
      this.maxOffset = getBoundaryOffset(LATEST) - 1;
    }

    @Override
    // Use binary search to find the offset of the message with the timestamp matching targetTimestamp.
    // Return the offset to fetch the next message until the matching message is found.
    public long onReceived(Iterator<FetchedMessage> messages) {
      if (!messages.hasNext()) {
        return EARLIEST;
      }
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
          try {
            Thread.sleep(1000);
          } catch (InterruptedException e) {
            LOG.info("Thread {} is interrupted while waiting for newer Kafka message.");
            Thread.currentThread().interrupt();
          }
          maxOffset = getBoundaryOffset(LATEST) - 1;
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

    /**
     * Gets earliest or latest message's offset
     */
    long getBoundaryOffset(long timeStamp) {
      // If no broker, treat it as failure attempt.
      if (simpleConsumer == null) {
        LOG.warn("Failed to talk to any broker. Default offset to 0 for {}-{}", topic, partition);
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
          simpleConsumer = new kafka.javaapi.consumer.SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                                                                     SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");
          LOG.warn("Failed to fetch offset for {}-{} with timestamp {}. Error: {}. Default offset to 0.",
                   topic, partition, timeStamp, errorCode);
        }
        return 0L;
      }

      return offsets[0];
    }
  }
}
