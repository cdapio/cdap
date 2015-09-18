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

import com.google.common.collect.Maps;
import org.apache.twill.kafka.client.TopicPartition;

import java.util.Collections;
import java.util.Map;

/**
 * Default implementation of {@link KafkaConfigurer}.
 */
final class DefaultKafkaConfigurer implements KafkaConfigurer {

  private String zookeeper;
  private String brokers;
  private final Map<TopicPartition, Integer> topicPartitions = Maps.newHashMap();

  @Override
  public void setZooKeeper(String zookeeper) {
    this.zookeeper = zookeeper;
  }

  @Override
  public void setBrokers(String brokers) {
    this.brokers = brokers;
  }

  @Override
  public void addTopicPartition(String topic, int partition) {
    addTopicPartition(topic, partition, DEFAULT_FETCH_SIZE);
  }

  @Override
  public void addTopicPartition(String topic, int partition, int fetchSize) {
    topicPartitions.put(new TopicPartition(topic, partition), fetchSize);
  }

  String getBrokers() {
    return brokers;
  }

  String getZookeeper() {
    return zookeeper;
  }

  Map<TopicPartition, Integer> getTopicPartitions() {
    return Collections.unmodifiableMap(topicPartitions);
  }
}
