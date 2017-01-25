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
import co.cask.cdap.logging.appender.kafka.LoggingEventSerializer;
import com.google.common.collect.ImmutableMap;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.OffsetResponse;
import org.apache.avro.generic.GenericRecord;
import org.apache.twill.common.Cancellable;
import org.apache.twill.internal.kafka.client.ZKBrokerService;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.apache.twill.kafka.client.FetchedMessage;
import org.apache.twill.kafka.client.KafkaConsumer;
import org.apache.twill.zookeeper.ZKClientService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

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
  private final LoggingEventSerializer serializer;

  public KafkaOffsetFinder(ZKClientService zkClientService, KafkaConsumer consumer) throws IOException {
    this.brokerService = new ZKBrokerService(zkClientService);
    this.brokerService.startAndWait();
    this.consumer = consumer;
    this.serializer = new LoggingEventSerializer();
  }

  public long getMatchingOffset(Checkpoint checkpoint, String topic, int partition) {
    final BlockingQueue<Long> offsetQueue = new LinkedBlockingQueue<>();
    Cancellable initCancel = consumer.prepare().add(topic, partition, checkpoint.getNextOffset())
      .consume(new OffsetFinderCallback(offsetQueue, checkpoint.getMaxEventTime(), topic, partition));
    long startOffset = EARLIEST;
    try {
      startOffset = offsetQueue.poll(360, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      LOG.error("Failed to find matching offset from Kafka. Return default value {}", EARLIEST, e);
    }
    initCancel.cancel();
    return startOffset;
  }

  private class OffsetFinderCallback implements KafkaConsumer.MessageCallback {

    private final BlockingQueue<Long> offsetQueue;
    private final long targetTimestamp;
    private final String topic;
    private final int partition;
    private final kafka.javaapi.consumer.SimpleConsumer simpleConsumer;

    private long minOffset;
    private long maxOffset;

    OffsetFinderCallback(final BlockingQueue<Long> offsetQueue, long targetTimestamp, String topic, int partition) {
      this.offsetQueue = offsetQueue;
      this.targetTimestamp = targetTimestamp;
      this.topic = topic;
      this.partition = partition;
      this.minOffset = EARLIEST; // earliest offset is the same as EARLIEST
      this.maxOffset = LATEST; // latest offset is the same as LATEST
      BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
      this.simpleConsumer
        = new kafka.javaapi.consumer.SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                                                    SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");
    }

    @Override
    // Use binary search to find the offset of the message with the index matching startIdx. Returns the next offset
    // to fetch message until the matching message is found.
    public long onReceived(Iterator<FetchedMessage> messages) {
      if (!messages.hasNext()) {
        return EARLIEST;
      }
      FetchedMessage message = messages.next();
      long offset = message.getOffset();
      GenericRecord genericRecord = serializer.toGenericRecord(message.getPayload());
      ILoggingEvent event = serializer.fromGenericRecord(genericRecord);
      LOG.trace("Got event {} for partition {}", event, partition);
      long timeStamp = event.getTimeStamp();
      if (timeStamp == targetTimestamp) {
        if (offsetQueue.size() == 0) {
          offsetQueue.offer(offset);
        }
        return offset;
      }
      // If minOffset and maxOffset still have their initial values, set the minOffset to offset and return
      // the offset of the last received message
      if (minOffset == EARLIEST && maxOffset == LATEST) {
        minOffset = offset;
        LOG.info("minOffset = {}, return maxOffset = {}", minOffset, maxOffset);
        // Returns the offset of the last received messages. Cannot return -1 because -1 will be translated as
        // the next offset after the last received message
        return getLastOffset() - 1;
      }
      if (maxOffset == LATEST) {
        maxOffset = offset;
      }
      LOG.info("minOffset = {}, maxOffset = {}", minOffset, maxOffset);
      // If minOffset > maxOffset, the startIdx cannot be found in the current range of offset.
      // Restore the initial values of minOffset and maxOffset and read from the beginning again
      if (minOffset > maxOffset) {
        minOffset = EARLIEST;
        maxOffset = LATEST;
        LOG.info("minOffset > maxOffset, return minOffset = {}", minOffset);
        return minOffset;
      }
      if (timeStamp > targetTimestamp) {
        maxOffset = offset - 1;
        long newOffset = minOffset + (maxOffset - minOffset) / 2;
        LOG.info("currentIdx > startIdx, return newOffset {}", newOffset);
        return newOffset;
      }
      if (timeStamp < targetTimestamp) {
        minOffset = offset + 1;
        long newOffset = minOffset + (maxOffset - minOffset) / 2;
        LOG.info("currentIdx < startIdx, return newOffset {}", newOffset);
        return newOffset;
      }
      return LATEST;
    }

    @Override
    public void finished() {
      //no-op
    }

    private long getLastOffset() {
      // If no broker, treat it as failure attempt.
      if (simpleConsumer == null) {
        LOG.warn("Failed to talk to any broker. Default offset to 0 for {}-{}", topic, partition);
        return 0L;
      }

      // Fire offset request
      kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(ImmutableMap.of(
        new TopicAndPartition(topic, partition),
        new PartitionOffsetRequestInfo(LATEST, 1)
      ), kafka.api.OffsetRequest.CurrentVersion(), simpleConsumer.clientId());

      OffsetResponse response = simpleConsumer.getOffsetsBefore(request);

      // Retrieve offsets from response
      long[] offsets = response.hasError() ? null : response.offsets(topic, partition);
      if (offsets == null || offsets.length <= 0) {
        short errorCode = response.errorCode(topic, partition);

        // If the topic partition doesn't exists, use offset 0 without logging error.
        if (errorCode != ErrorMapping.UnknownTopicOrPartitionCode()) {
          LOG.warn("Failed to fetch offset for {}-{} with timestamp {}. Error: {}. Default offset to 0.",
                   topic, partition, LATEST, errorCode);
        }
        return 0L;
      }

      LOG.debug("Offset {} fetched for {}-{} with timestamp {}.", offsets[0], topic, partition, LATEST);
      return offsets[0];
    }
  }
}
