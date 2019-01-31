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

import co.cask.cdap.logging.pipeline.LogPipelineConfig;
import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A class to hold configurations for a {@link KafkaLogProcessorPipeline}.
 */
public final class KafkaPipelineConfig extends LogPipelineConfig {

  private final String topic;
  private final Set<Integer> partitions;
  private final int kafkaFetchBufferSize;

  public KafkaPipelineConfig(String topic, Set<Integer> partitions, long maxBufferSize,
                             long eventDelayMillis, int kafkaFetchBufferSize, long checkpointIntervalMillis) {
    super(maxBufferSize, eventDelayMillis, checkpointIntervalMillis);
    this.topic = topic;
    this.partitions = ImmutableSet.copyOf(partitions);
    this.kafkaFetchBufferSize = kafkaFetchBufferSize;
  }

  String getTopic() {
    return topic;
  }

  Set<Integer> getPartitions() {
    return partitions;
  }

  int getKafkaFetchBufferSize() {
    return kafkaFetchBufferSize;
  }

  @Override
  public String toString() {
    return "KafkaPipelineConfig{" +
      "topic='" + topic + '\'' +
      ", partitions=" + partitions +
      ", maxBufferSize=" + getMaxBufferSize() +
      ", eventDelayMillis=" + getEventDelayMillis() +
      ", kafkaFetchBufferSize=" + kafkaFetchBufferSize +
      ", checkpointIntervalMillis=" + getCheckpointIntervalMillis() +
      '}';
  }
}
