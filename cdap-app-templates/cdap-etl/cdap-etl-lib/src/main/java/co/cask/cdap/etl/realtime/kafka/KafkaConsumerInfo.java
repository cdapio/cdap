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

package co.cask.cdap.etl.realtime.kafka;

import org.apache.twill.kafka.client.TopicPartition;

/**
 * Helper class to carry information about a Kafka consumer of a particular topic partition.
 *
 * @param <OFFSET> Type of the offset object
 */
public final class KafkaConsumerInfo<OFFSET> {
  private final TopicPartition topicPartition;
  private final int fetchSize;
  private OFFSET readOffset;
  private OFFSET pendingReadOffset;

  public KafkaConsumerInfo(TopicPartition topicPartition, int fetchSize, OFFSET readOffset) {
    this.topicPartition = topicPartition;
    this.fetchSize = fetchSize;
    this.readOffset = readOffset;
  }

  public TopicPartition getTopicPartition() {
    return topicPartition;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public OFFSET getReadOffset() {
    return pendingReadOffset != null ? pendingReadOffset : readOffset;
  }

  public void setReadOffset(OFFSET readOffset) {
    this.pendingReadOffset = readOffset;
  }

  public boolean hasPendingChanges() {
    return this.pendingReadOffset != null;
  }

  void commitReadOffset() {
    if (pendingReadOffset != null) {
      readOffset = pendingReadOffset;
      pendingReadOffset = null;
    }
  }

  void rollbackReadOffset() {
    pendingReadOffset = null;
  }
}
