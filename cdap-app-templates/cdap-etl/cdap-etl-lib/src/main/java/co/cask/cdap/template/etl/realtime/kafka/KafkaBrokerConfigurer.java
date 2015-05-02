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

/**
 * This class is for configuring Kafka broker information for {@link KafkaSimpleApiConsumer}.
 */
public interface KafkaBrokerConfigurer {

  /**
   * Sets the ZooKeeper quorum string that Kafka is running with. If this is set, then ZooKeeper will be used
   * for discovery of Kafka brokers, regardless of what's being set by {@link #setBrokers(String)}.
   */
  void setZooKeeper(String zookeeper);

  /**
   * Sets the Kafka broker list. The format of the broker list is based on the Kafka version.
   */
  void setBrokers(String brokers);
}
