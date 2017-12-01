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

import co.cask.cdap.common.NotFoundException;
import co.cask.cdap.logging.meta.Checkpoint;
import co.cask.cdap.logging.serialize.LoggingEventSerializer;
import com.google.common.base.Preconditions;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.LeaderNotAvailableException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.twill.kafka.client.BrokerInfo;
import org.apache.twill.kafka.client.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;

/**
 * Resolve matching Kafka offset with given checkpoint.
 */
class KafkaOffsetResolver {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaOffsetResolver.class);

  // TODO: (CDAP-8439) determine the appropriate size
  private static final int SINGLE_MESSAGE_MAX_SIZE = 50 * 1024;        // The maximum size of a single message
  private static final int BUFFER_SIZE = 1000 * SINGLE_MESSAGE_MAX_SIZE;
  private static final int SO_TIMEOUT_MILLIS = 5 * 1000;           // 5 seconds.

  private final BrokerService brokerService;
  private final KafkaPipelineConfig config;
  private final LoggingEventSerializer serializer;

  KafkaOffsetResolver(BrokerService brokerService, KafkaPipelineConfig config) {
    this.brokerService = brokerService;
    this.config = config;
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
   * @param partition  the partition in the topic for searching matching offset
   * @return the next offset of the message with smallest offset and log event time equal to
   * {@code checkpoint.getNextEventTime()}.
   * {@code -1} if no such offset can be found or {@code checkpoint.getNextOffset()} is negative.
   *
   * @throws LeaderNotAvailableException if there is no Kafka broker to talk to.
   * @throws OffsetOutOfRangeException if the given offset is out of range.
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  long getStartOffset(final Checkpoint checkpoint, final int partition) {
    // This should never happen
    Preconditions.checkArgument(checkpoint.getNextOffset() > 0, "Invalid checkpoint offset");

    // Get BrokerInfo for constructing SimpleConsumer
    String topic = config.getTopic();
    BrokerInfo brokerInfo = brokerService.getLeader(topic, partition);
    if (brokerInfo == null) {
      throw new LeaderNotAvailableException(
        String.format("BrokerInfo from BrokerService is null for topic %s partition %d. Will retry in next run.",
                      topic, partition));
    }
    SimpleConsumer consumer = new SimpleConsumer(brokerInfo.getHost(), brokerInfo.getPort(),
                                                 SO_TIMEOUT_MILLIS, BUFFER_SIZE,
                                                 "offset-finder-" + topic + "-" + partition);

    // Check whether the message fetched with the offset in the given checkpoint has the timestamp from
    // checkpoint.getNextOffset() - 1 to get the offset corresponding to the timestamp in checkpoint
    long offset = checkpoint.getNextOffset() - 1;
    try {
      long timestamp = getEventTimeByOffset(consumer, partition, offset);
      if (timestamp == checkpoint.getNextEventTime()) {
        return checkpoint.getNextOffset();
      }
      // This can happen in replicated cluster
      LOG.debug("Event timestamp in {}:{} at offset {} is {}. It doesn't match with checkpoint timestamp {}",
                topic, partition, offset, timestamp, checkpoint.getNextEventTime());
    } catch (NotFoundException | OffsetOutOfRangeException e) {
      // This means we can't find the timestamp. This can happen in replicated cluster
      LOG.debug("Cannot get valid log event in {}:{} at offset {}", topic, partition, offset);
    }

    // Find offset that has an event that matches the timestamp
    long nextOffset = findStartOffset(consumer, partition, checkpoint.getNextEventTime());
    LOG.debug("Found new nextOffset {} for topic {} partition {} with existing checkpoint {}.",
              nextOffset, topic, partition, checkpoint);
    return nextOffset;
  }

  /**
   *
   * Performs a linear search to find the next offset of the message with smallest offset and log event time
   * equal to {@code targetTime}. Stop searching when the current message has log event time
   * later than {@code maxTime} or offset larger than {@code maxOffset}
   *
   * @return next offset of the message with smallest offset and log event time equal to targetTime,
   *         or next offset of the message with largest offset and timestamp smaller than
   *         (targetTime - EVENT_DELAY_MILLIS) if no message has log event time equal to targetTime,
   *         or startOffset if no event has log event time smaller than (targetTime - EVENT_DELAY_MILLIS)
   * @throws OffsetOutOfRangeException if the given offset is out of range.
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  private long findStartOffset(SimpleConsumer consumer, int partition, long targetTime) throws KafkaException {
    String topic = config.getTopic();
    long minTime = targetTime - config.getEventDelayMillis();
    // The latest event time time we encounter before bailing the search
    long maxTime = targetTime + config.getEventDelayMillis();

    // The offset to start the search from
    long offset = KafkaUtil.getOffsetByTimestamp(consumer, topic, partition, minTime);
    long closestOffset = offset;

    boolean done = false;
    while (!done) {
      ByteBufferMessageSet messageSet = KafkaUtil.fetchMessages(consumer, topic, partition,
                                                              config.getKafkaFetchBufferSize(), offset);
      done = true;
      for (MessageAndOffset messageAndOffset : messageSet) {
        done = false;

        offset = messageAndOffset.nextOffset();
        try {
          long timestamp = serializer.decodeEventTimestamp(messageAndOffset.message().payload());
          if (timestamp == targetTime) {
            LOG.debug("Matching offset found in {}:{} at {} for timestamp {}",
                      topic, partition, messageAndOffset.offset(), targetTime);
            return offset;
          }

          if (timestamp < minTime) {
            closestOffset = offset;
          }

          if (timestamp > maxTime) {
            done = true;
            break;
          }
        } catch (IOException e) {
          // This shouldn't happen. In case it happens (e.g. someone published some garbage), just skip the message.
          LOG.trace("Fail to decode logging event time {}:{} at offset {}. Skipping it.",
                    topic, partition, messageAndOffset.offset(), e);
        }
      }
    }

    LOG.debug("Fail to find a log event with timestamp {} in {}:{}. " +
              "The largest offset with event timestamp smaller than {} (target event time minus event delay {}) is {}",
              targetTime, topic, partition, minTime, config.getEventDelayMillis(), closestOffset);
    return closestOffset;
  }

  /**
   * Fetch a log event with {@code requestOffset} and deserialize it to get the log event time.
   *
   * @return the log event time of the message with {@code requestOffset}
   * @throws NotFoundException If cannot find a valid log event message at the given offset
   * @throws OffsetOutOfRangeException if the given offset is out of range.
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  private long getEventTimeByOffset(SimpleConsumer consumer,
                                    int partition, long requestOffset) throws NotFoundException {
    String topic = config.getTopic();
    ByteBufferMessageSet messageSet = KafkaUtil.fetchMessages(consumer, topic, partition,
                                                              SINGLE_MESSAGE_MAX_SIZE, requestOffset);
    Iterator<MessageAndOffset> iterator = messageSet.iterator();
    if (!iterator.hasNext()) {
      throw new NotFoundException("No message found in " + topic + ":" + partition + " at offset " + requestOffset);
    }
    try {
      return serializer.decodeEventTimestamp(iterator.next().message().payload());
    } catch (IOException e) {
      // Fail to deserialize is the same as not found because in anywhere this is not the event we are looking for
      throw new NotFoundException("Invalid log event found in " + topic + ":" + partition +
                                    " at offset " + requestOffset);
    }
  }
}
