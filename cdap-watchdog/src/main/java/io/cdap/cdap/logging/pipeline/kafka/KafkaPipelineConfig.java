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

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A class to hold configurations for a {@link KafkaLogProcessorPipeline}.
 */
public final class KafkaPipelineConfig {

  private final String topic;
  private final Set<Integer> partitions;
  private final long maxBufferSize;
  private final long eventDelayMillis;
  private final int kafkaFetchBufferSize;
  private final long checkpointIntervalMillis;

  public KafkaPipelineConfig(String topic, Set<Integer> partitions, long maxBufferSize,
                             long eventDelayMillis, int kafkaFetchBufferSize, long checkpointIntervalMillis) {
    this.topic = topic;
    this.partitions = ImmutableSet.copyOf(partitions);
    this.maxBufferSize = maxBufferSize;
    this.eventDelayMillis = eventDelayMillis;
    this.kafkaFetchBufferSize = kafkaFetchBufferSize;
    this.checkpointIntervalMillis = checkpointIntervalMillis;
  }

  String getTopic() {
    return topic;
  }

  Set<Integer> getPartitions() {
    return partitions;
  }

  long getMaxBufferSize() {
    return maxBufferSize;
  }

  long getEventDelayMillis() {
    return eventDelayMillis;
  }

  int getKafkaFetchBufferSize() {
    return kafkaFetchBufferSize;
  }

  long getCheckpointIntervalMillis() {
    return checkpointIntervalMillis;
  }

  @Override
  public String toString() {
    return "KafkaPipelineConfig{" +
      "topic='" + topic + '\'' +
      ", partitions=" + partitions +
      ", maxBufferSize=" + maxBufferSize +
      ", eventDelayMillis=" + eventDelayMillis +
      ", kafkaFetchBufferSize=" + kafkaFetchBufferSize +
      ", checkpointIntervalMillis=" + checkpointIntervalMillis +
      '}';
  }
}
