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
class KafkaOffsetResolver {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetResolver.class);

  // TODO: (CDAP-8439) determine the appropriate size
  private static final int FETCH_SIZE = 100 * 1024;        // Use a default fetch size.
  private static final int SO_TIMEOUT_MILLIS = 5 * 1000;           // 5 seconds.
  private final long replicationDelayMillis;
  private final long eventOutOfOrderMillis;

  private final BrokerService brokerService;
  private final String topic;
  private final LoggingEventSerializer serializer;

  KafkaOffsetResolver(BrokerService brokerService, KafkaPipelineConfig config) {
    this.brokerService = brokerService;
    this.topic = config.getTopic();
    this.eventOutOfOrderMillis = config.getEventOutOfOrderMillis();
    this.replicationDelayMillis = config.getReplicationDelayMillis();
    this.serializer = new LoggingEventSerializer();
  }

  /**
   * Check whether the message fetched with the offset {@code checkpoint.getNextOffset() - 1} contains the
   * same timestamp as in the given checkpoint. If they match, directly return {@code checkpoint.getNextOffset()}.
   * If they don't, search for the smallest offset of the message with the same log event time
   * as {@code checkpoint.getNextEventTime()}
   *
   * @param checkpoint A {@link Checkpoint} containing the next offset of a message and its log event timestamp.
   *                   {@link Checkpoint#getNextOffset()}, {@link Checkpoint#getNextEventTime()}
   *                   and {@link Checkpoint#getMaxEventTime()} all must return a non-negative long
   * @param partition the partition in the topic for searching matching offset
   * @return the next offset of the message with smallest offset and log event time equal to
   *         {@code checkpoint.getNextEventTime()}.
   *         {@code -1} if no such offset can be found or {@code checkpoint.getNextOffset()} is negative.
   */
   long getMatchingOffset(final Checkpoint checkpoint, final int partition) {
     // No message should have next offset 0.
     if (checkpoint.getNextOffset() == 0) {
       return -1;
     }
    // Get BrokerInfo for constructing SimpleConsumer in OffsetFinderCallback
    BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
    if (brokerInfo == null) {
      LOG.error("BrokerInfo from BrokerService is null for topic {} partition {}. Will retry in next run.",
                topic, partition);
      return kafka.api.OffsetRequest.EarliestTime(); // Return earliest offset if brokerInfo is null
    }
    SimpleConsumer simpleConsumer
      = new SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                           SO_TIMEOUT_MILLIS, FETCH_SIZE, "simple-kafka-client");

    String clientId = topic + "_" + partition;
    // Check whether the message fetched with the offset in the given checkpoint has the timestamp from
    // checkpoint.getNextOffset() - 1 to get the offset corresponding to the timestamp in checkpoint
    long timeStamp = getEventTimeByOffset(simpleConsumer, partition, clientId, checkpoint.getNextOffset() - 1);
    if (timeStamp == checkpoint.getNextEventTime()) {
      return checkpoint.getNextOffset();
    }

    long smallestMatchingOffset = findOffsetByTime(simpleConsumer, partition, clientId, checkpoint.getNextEventTime());
    if (smallestMatchingOffset >= 0) {
      // Increment smallestMatchingOffset to get the next offset if it's a valid non-negative offset
      smallestMatchingOffset++;
    }
    return smallestMatchingOffset;
  }

  /**
   * Determine the lower-bound and upper-bound of offsets to search for the smallest offset of the message
   * with log event time equal to {@code targetTime}, and then perform the search.
   *
   * @return the matching offset or {@code -1} if not found
   */
  private long findOffsetByTime(SimpleConsumer consumer, int partition, String clientName, long targetTime) {
    TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
    Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<>();
    // the offset of the earliest message
    long minOffset = fetchOffsetByTime(consumer, partition, kafka.api.OffsetRequest.EarliestTime());
    // the next offset after the latest message
    long maxOffset = fetchOffsetByTime(consumer, partition, kafka.api.OffsetRequest.LatestTime());
    long endOffset = maxOffset - 1; // the offset of the latest message
    long prevOffset = Long.MAX_VALUE;
    // Start from (targetTime + replicationDelayMillis) to be safer
    long requestTime = targetTime + replicationDelayMillis;
    while (true) {
      requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(requestTime, 1));
      OffsetResponse response =
        consumer.getOffsetsBefore(new OffsetRequest(requestInfo, kafka.api.OffsetRequest.CurrentVersion(), clientName));
      // Fetch the starting offset of the last segment whose latest message is published before requestTime
      long[] responseOffsets = response.offsets(topic, partition);
      // If no offset is returned, the targetTime is earlier than all existing Kafka publishing time.
      if (responseOffsets.length == 0) {
        // Start searching from the earliest offset
        return searchOffsetByTime(consumer, partition, clientName, targetTime, minOffset, endOffset);
      }
      long offset = responseOffsets[0];
      // If offset == maxOffset, no message is received earlier than requestTime
      if (offset == maxOffset) {
        // If requestTime is targetTime + replicationDelayMillis, probably replicationDelayMillis is overestimated.
        // Start searching from targetTime instead;
        if (requestTime == targetTime + replicationDelayMillis) {
          requestTime = targetTime;
          continue;
        }
        // If targetTime is already requested, start searching from the earliest offset
        return searchOffsetByTime(consumer, partition, clientName, targetTime, minOffset, endOffset);
      }
      // Fetch the message with offset and get its event time
      long eventTime = getEventTimeByOffset(consumer, partition, clientName, offset);

      // If eventTime is smaller than 0, getting eventTime failed. Start searching from the earliest offset
      if (eventTime < 0) {
        return searchOffsetByTime(consumer, partition, clientName, targetTime, minOffset, endOffset);
      }

      // If the new offset is the same as the previous offset, we reach the earliest earliest message
      if (offset == prevOffset) {
        if (eventTime > targetTime + eventOutOfOrderMillis) {
          // If the event time of the earliest message is still later than targetTime + eventOutOfOrderMillis,
          // probably no message in this topic-partition contains an event time equal to targetTime.
          // Search from minOffset for safety
          return searchOffsetByTime(consumer, partition, clientName, targetTime, minOffset, endOffset);
        } else {
          return searchOffsetByTime(consumer, partition, clientName, targetTime, offset, endOffset);
        }
      }
      // If eventTime is earlier than targetTime by more than eventOutOfOrderMillis, start searching for the
      // the offset corresponding the targetTime should be with the range of [offset, endOffset]
      if (eventTime < targetTime - eventOutOfOrderMillis) {
        return searchOffsetByTime(consumer, partition, clientName, targetTime, offset, endOffset);
      } else if (eventTime > targetTime + eventOutOfOrderMillis) {
        // if eventTime is later than targetTime by more than eventOutOfOrderMillis, update the endOffset to
        // the current offset
        endOffset = offset;
      }
      requestTime = eventTime;
      prevOffset = offset;
    }
  }

  /**
   * Performs a binary search to find the smallest offset of the message between {@code startOffset}
   * and {@code endOffset} with log event time equal to {@code targetTime}.
   *
   * @return the matching offset or {@code -1} if not found
   */
  private long searchOffsetByTime(SimpleConsumer consumer, int partition, String clientName, long targetTime,
                                  long startOffset, long endOffset) {
    long minOffset = startOffset;
    long maxOffset = endOffset;
    long offset;

    while (minOffset <= maxOffset) {
      offset = minOffset + (maxOffset - minOffset) / 2;
      long timeStamp = getEventTimeByOffset(consumer, partition, clientName, offset);
      // if timeStamp is within the range of (targetTime - eventOutOfOrderMillis, targetTime + eventOutOfOrderMillis),
      // targetTime must be within the range of [timeStamp - eventOutOfOrderMillis, timeStamp + eventOutOfOrderMillis].
      // Perform a linear search within this range.
      if (Math.abs(timeStamp - targetTime) < eventOutOfOrderMillis) {
        return linearSearchAroundOffset(consumer, partition, clientName, offset, targetTime,
                                        timeStamp - eventOutOfOrderMillis, timeStamp + eventOutOfOrderMillis);
      }
      if (timeStamp > targetTime) {
        maxOffset = offset - 1;
      }
      if (timeStamp < targetTime) {
        minOffset = offset + 1;
      }
    }
    return -1;
  }

  /**
   * Performs a linear search to find the smallest offset of the message with log event time
   * equal to {@code targetTime}. Stop searching when the current message has log event time smaller than
   * {@code minTime} or larger than {@code maxTime}
   *
   * @return the matching offset or {@code -1} if not found
   */
  private long linearSearchAroundOffset(SimpleConsumer consumer, int partition, String clientName,
                                        long startOffset, long targetTime, long minTime, long maxTime) {
    long smallestMatchingOffset = -1;
    // search from startOffset and backward first
    long searchOffset = startOffset;
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
    searchOffset = startOffset;
    // The latest offset returned by fetchOffsetByTime is 1 larger than the offset of the latest received message
    long maxOffset = fetchOffsetByTime(consumer, partition, kafka.api.OffsetRequest.LatestTime());
    // search forward from (startOffset + 1) if no matching offset found when searching backward
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

  /**
   * Fetch a log event with {@code requestOffset} and deserialize it to get the log event time.
   *
   * @return the log event time of the message with {@code requestOffset} or {@code -1} if no message found.
   */
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
        return -1;
      }
      LOG.error("Failed to fetch message for topic: {} partition: {} with error code {}.",
                topic, partition, code);
      return -1;
    }
    for (MessageAndOffset messageAndOffset : fetchResponse.messageSet(topic, partition)) {
      long offset = messageAndOffset.offset();
      try {
        // TODO: [CDAP-8470] need a serializer for deserializing ILoggingEvent Kafka message to get time only
        ILoggingEvent event = serializer.fromBytes(messageAndOffset.message().payload());
        return event.getTimeStamp();
      } catch (IOException e) {
        LOG.warn("Message with offset {} in topic {} partition {} is ignored because of failure to decode.",
                 offset, topic, partition, e);
      }
    }
    return -1;
  }

  /**
   * Fetch the starting offset of the last segment whose latest message is published before the given {@code timeStamp}
   */
  private long fetchOffsetByTime(SimpleConsumer simpleConsumer, int partition, long timeStamp) {
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
      // If the topic partition doesn't exist, use offset 0 without logging error.
      if (errorCode != ErrorMapping.UnknownTopicOrPartitionCode()) {
        LOG.error("Failed to fetch offset for {}-{} with timestamp {}. Error: {}. Default offset to 0.",
                  topic, partition, timeStamp, errorCode);
      }
      return 0L;
    }

    return offsets[0];
  }
}
