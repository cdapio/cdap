/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.realtime.kafka;

import org.apache.twill.kafka.client.TopicPartition;

import java.nio.ByteBuffer;

/**
 * Represents a Kafka message.
 *
 * @param <OFFSET> Type of message offset
 */
public final class KafkaMessage<OFFSET> {

  private final TopicPartition topicPartition;
  private final OFFSET nextOffset;
  private final ByteBuffer key;
  private final ByteBuffer payload;

  public KafkaMessage(TopicPartition topicPartition, OFFSET nextOffset, ByteBuffer key, ByteBuffer payload) {
    this.topicPartition = topicPartition;
    this.nextOffset = nextOffset;
    this.key = key;
    this.payload = payload;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public OFFSET getNextOffset() {
    return nextOffset;
  }

  public ByteBuffer getKey() {
    return key;
  }

  public ByteBuffer getPayload() {
    return payload;
  }
}
