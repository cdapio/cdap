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

import com.google.common.collect.ImmutableMap;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.OffsetRequest$;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.NotLeaderForPartitionException;
import org.apache.kafka.common.errors.OffsetOutOfRangeException;
import org.apache.kafka.common.errors.UnknownServerException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.protocol.Errors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for Kafka.
 */
public final class KafkaUtil {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);

  /**
   * Fetch the starting offset of the last segment whose latest message is published before the given timestamp.
   * The timestamp can also be special value {@link OffsetRequest$#EarliestTime()}
   * or {@link OffsetRequest$#LatestTime()}.
   *
   * @param consumer the consumer to send request to and receive response from
   * @param topic the topic for fetching the offset from
   * @param partition the partition for fetching the offset from
   * @param timestamp the timestamp to use for fetching last offset before it
   * @return the latest offset
   *
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  public static long getOffsetByTimestamp(SimpleConsumer consumer, String topic,
                                          int partition, long timestamp) throws KafkaException {
    // Fire offset request
    OffsetRequest request = new OffsetRequest(ImmutableMap.of(
      new TopicAndPartition(topic, partition),
      new PartitionOffsetRequestInfo(timestamp, 1)
    ), kafka.api.OffsetRequest.CurrentVersion(), consumer.clientId());

    OffsetResponse response = consumer.getOffsetsBefore(request);

    if (response.hasError()) {
      throw Errors.forCode(response.errorCode(topic, partition)).exception();
    }

    // Retrieve offsets from response
    long[] offsets = response.offsets(topic, partition);
    if (offsets.length == 0) {
      if (timestamp != kafka.api.OffsetRequest.EarliestTime()) {
        // If offsets length is 0, that means the timestamp we are looking for is in the first Kafka segment
        // Hence, use the earliest time to find out the offset
        return getOffsetByTimestamp(consumer, topic, partition, kafka.api.OffsetRequest.EarliestTime());
      }
      // This shouldn't happen. The find earliest offset response should return at least one offset.
      throw new UnknownServerException("Empty offsets received from offsets request on " + topic + ":" + partition +
                                         " from broker " + consumer.host() + ":" + consumer.port());
    }

    LOG.debug("Offset {} fetched for {}:{} with timestamp {}.", offsets[0], topic, partition, timestamp);
    return offsets[0];
  }

  /**
   * Fetches messages from the given topic, partition and offset using the provided {@link SimpleConsumer}.
   *
   * @return A {@link ByteBufferMessageSet} of the given topic, partition for messages fetched
   *
   * @throws OffsetOutOfRangeException if the given offset is out of range.
   * @throws NotLeaderForPartitionException if the broker that the consumer is talking to is not the leader
   *                                        for the given topic and partition.
   * @throws UnknownTopicOrPartitionException if the topic or partition is not known by the Kafka server
   * @throws UnknownServerException if the Kafka server responded with error.
   */
  public static ByteBufferMessageSet fetchMessages(SimpleConsumer consumer, String topic,
                                                   int partition, int fetchSize,
                                                   long requestOffset) throws KafkaException {
    FetchRequest req = new FetchRequestBuilder()
      .clientId(consumer.clientId())
      .addFetch(topic, partition, requestOffset, fetchSize)
      .build();
    FetchResponse fetchResponse = consumer.fetch(req);

    if (fetchResponse.hasError()) {
      throw Errors.forCode(fetchResponse.errorCode(topic, partition)).exception();
    }

    return fetchResponse.messageSet(topic, partition);
  }

  private KafkaUtil() {
    // no-op
  }
}
