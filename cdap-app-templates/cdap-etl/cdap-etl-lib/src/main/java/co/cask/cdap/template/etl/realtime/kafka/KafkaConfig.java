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

import javax.annotation.Nullable;

/**
 * Contains information about Kafka cluster as configured by user.
 */
public final class KafkaConfig {

  private final String zookeeper;
  private final String brokers;

  public KafkaConfig(String zookeeper, String brokers) {
    this.zookeeper = zookeeper;
    this.brokers = brokers;
  }

  /**
   * Returns the ZooKeeper connection string
   * or {@code null}.
   */
  @Nullable
  public String getZookeeper() {
    return zookeeper;
  }

  /**
   * Returns brokers information
   */
  @Nullable
  public String getBrokers() {
    return brokers;
  }
}
