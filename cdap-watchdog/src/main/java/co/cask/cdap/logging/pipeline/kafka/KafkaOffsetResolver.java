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
import com.google.common.collect.ImmutableMap;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Resolve matching Kafka offset with given checkpoint.
 */
public class KafkaOffsetResolver {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetResolver.class);

  // TODO determine the appropriate size
  private static final int FETCH_SIZE = 1024 * 1024;        // Use a default fetch size.
  private static final int SO_TIMEOUT = 5 * 1000;           // 5 seconds.
  private static final long LATEST = kafka.api.OffsetRequest.LatestTime();
  private static final long EARLIEST = kafka.api.OffsetRequest.EarliestTime();
  private static final long REPLICATION_DELAY = 5 * 60 * 1000;
  private static final long MAX_TIME_GAP = 60 * 1000;

  private final BrokerService brokerService;
  private final String topic;
  private final LoggingEventSerializer serializer;

  public KafkaOffsetResolver(BrokerService brokerService, String topic) throws IOException {
    this.brokerService = brokerService;
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
    SimpleConsumer simpleConsumer
      = new SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                           SO_TIMEOUT, FETCH_SIZE, "simple-kafka-client");

    String clientId = topic + "_" + partition;
    // Check whether the message fetched with the offset in the given checkpoint has the timestamp from
    // checkpoint.getNextOffset() - 1 to get the offset corresponding to the timestamp in checkpoint
    long timeStamp = getEventTimeByOffset(simpleConsumer, partition, clientId, checkpoint.getNextOffset() - 1);
    if (timeStamp == checkpoint.getNextEventTime()) {
      return checkpoint.getNextOffset();
    }

    return findOffsetByTime(simpleConsumer, partition, clientId, checkpoint.getNextEventTime());
  }

  private long findOffsetByTime(SimpleConsumer consumer, int partition, String clientName, long targetTimestamp) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    long minOffset = fetchOffsetByTime(consumer, partition, EARLIEST); // the offset of the earliest message
    long maxOffset = fetchOffsetByTime(consumer, partition, LATEST); // the next offset after the latest message
    long endOffset = maxOffset - 1; // the offset of the latest message
    long prevOffset = Long.MAX_VALUE;
    long requestTime = targetTimestamp + REPLICATION_DELAY;
    while (true) {
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(requestTime, 1));
      OffsetResponse response =
        consumer.getOffsetsBefore(new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName));
      // Fetch the starting offset of the first segment whose latest message is received
      // at the time earlier than requestTime
      long[] responseOffsets = response.offsets(topic, partition);
      long offset = responseOffsets[0];
      // If no message is received earlier than requestTime, start searching for the offset from the earliest offset
      if (offset == maxOffset) {
        return searchOffsetByTime(consumer, partition, clientName, targetTimestamp, minOffset, endOffset);
      }
      // Fetch the message with offset and get its event time
      long eventTime = getEventTimeByOffset(consumer, partition, clientName, offset);

      // If eventTime is smaller than 0, getting eventTime failed. Start searching from the earliest offset
      if (eventTime < 0) {
        return searchOffsetByTime(consumer, partition, clientName, targetTimestamp, minOffset, endOffset);
      }

      // If the new offset is the same as the previous offset, we reach the earliest earliest message
      if (offset == prevOffset) {
        if (eventTime > targetTimestamp + MAX_TIME_GAP) {
          // If the event time of the earliest message is still later than targetTimestamp + MAX_TIME_GAP,
          // probably no message in this topic-partition contains an event time equal to targetTimestamp.
          // Search from minOffset for safety
          return searchOffsetByTime(consumer, partition, clientName, targetTimestamp, minOffset, endOffset);
        } else {
          return searchOffsetByTime(consumer, partition, clientName, targetTimestamp, offset, endOffset);
        }
      }
      // If eventTime is earlier than targetTimestamp by more than MAX_TIME_GAP, start searching for the
      // the offset corresponding the targetTimestamp should be with the range of [offset, endOffset]
      if (eventTime < targetTimestamp - MAX_TIME_GAP) {
        return searchOffsetByTime(consumer, partition, clientName, targetTimestamp, offset, endOffset);
      } else if (eventTime > targetTimestamp + MAX_TIME_GAP) {
        // if eventTime is later than targetTimestamp by more than MAX_TIME_GAP, update the endOffset to
        // the current offset
        endOffset = offset;
      }
      requestTime = eventTime;
      prevOffset = offset;
    }
  }

  private long searchOffsetByTime(SimpleConsumer consumer, int partition, String clientName, long targetTimestamp,
                                  long startOffset, long endOffset) {
    long minOffset = startOffset;
    long maxOffset = endOffset;
    long offset;

    while (minOffset <= maxOffset) {
      offset = minOffset + (maxOffset - minOffset) / 2;
      long timeStamp = getEventTimeByOffset(consumer, partition, clientName, offset);
      // if timeStamp is within the range of (targetTimestamp - MAX_TIME_GAP, targetTimestamp + MAX_TIME_GAP),
      // targetTimestamp must be within the range of [timeStamp - MAX_TIME_GAP, timeStamp + MAX_TIME_GAP].
      // Perform a linear search within this range.
      if (Math.abs(timeStamp - targetTimestamp) < MAX_TIME_GAP) {
        return linearSearchAroundOffset(consumer, partition, clientName, offset, targetTimestamp,
                                        timeStamp - MAX_TIME_GAP, timeStamp + MAX_TIME_GAP);
      }
      if (timeStamp > targetTimestamp) {
        maxOffset = offset - 1;
      }
      if (timeStamp < targetTimestamp) {
        minOffset = offset + 1;
      }
    }
    return -1;
  }

  private long linearSearchAroundOffset(SimpleConsumer consumer, int partition, String clientName,
                                        long offset, long targetTime, long minTime, long maxTime) {
    long smallestMatchingOffset = -1;
    // search backward first
    long searchOffset = offset;
    while (searchOffset >= 0) {
      long time = getEventTimeByOffset(consumer, partition, clientName, searchOffset);
      if (time < minTime) {
        break;
      }
      if (time == targetTime) {
        smallestMatchingOffset = searchOffset;
      }
      searchOffset--;
    }
    if (smallestMatchingOffset != -1) {
      return smallestMatchingOffset;
    }
    // search forward if no matching offset found when searching backward
    searchOffset = offset;

    // The latest offset returned by consumer is 1 larger than the offset of the latest received message
    long maxOffset = fetchOffsetByTime(consumer, partition, LATEST);
    while (++searchOffset < maxOffset) {
      long time = getEventTimeByOffset(consumer, partition, clientName, searchOffset);
      if (time == targetTime) {
        return searchOffset;
      }
      if (time > maxTime) {
        return -1;
      }
    }
    return -1;
  }

  private long getEventTimeByOffset(SimpleConsumer consumer, int partition, String clientId, long requestOffset) {
    FetchRequest req = new FetchRequestBuilder()
      .clientId(clientId).addFetch(topic, partition, requestOffset, 100000).build();
    FetchResponse fetchResponse = consumer.fetch(req);

    if (fetchResponse.hasError()) {
      // Something went wrong!
      short code = fetchResponse.errorCode(topic, partition);
      if (code == ErrorMapping.OffsetOutOfRangeCode()) {
        // We asked for an invalid offset. For simple case ask for the last element to reset
        LOG.debug("FetchRequest offset: {} out of range.", requestOffset);
      }
      LOG.error("Failed to fetch message for topic: {} partition: {} with error code {}.",
                topic, partition, code);
    } else {
      for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
        long offset = messageAndOffset.offset();
        if (offset < requestOffset) {
          LOG.error("Found an old offset: {} Expecting: {}", offset, requestOffset);
          continue;
        }
        try {
          ILoggingEvent event = serializer.fromBytes(messageAndOffset.message().payload());
          return event.getTimeStamp();
        } catch (IOException e) {
          LOG.warn("Message with offset {} in topic {} partition {} is ignored because of failure to decode.",
                   offset, topic, partition, e);
        }
      }
    }
    return -1;
  }

  /**
   * Gets earliest or latest message's offset
   */
  private long fetchOffsetByTime(SimpleConsumer simpleConsumer, int partition, long timeStamp) {
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
