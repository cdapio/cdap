/*
 * Copyright © 2014 Cask Data, Inc.
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

package co.cask.cdap.guides.kafka;

import co.cask.cdap.api.flow.Flow;
import co.cask.cdap.api.flow.FlowSpecification;

/**
 * Flow to ingest Kafka Messages (works with Kafka Cluster v0.8.x).
 * <p>
 * Requires these runtime arguments:
 * <ul>
 * <li>kafka.zookeeper: Kafka Zookeeper connection string</li>
 * <li>kafka.topic: Subscribe to Kafka Topic</li>
 * </ul>
 * </p>
 */
public class KafkaIngestionFlow implements Flow {

  @Override
  public FlowSpecification configure() {
    return FlowSpecification.Builder.with()
      .setName(Constants.FLOW_NAME)
      .setDescription("Subscribes to Kafka messages")
      .withFlowlets()
        .add(Constants.KAFKA_FLOWLET, new KafkaConsumerFlowlet())
        .add(Constants.COUNTER_FLOWLET, new KafkaMessageCounterFlowlet())
      .connect()
        .from(Constants.KAFKA_FLOWLET).to(Constants.COUNTER_FLOWLET)
      .build();
  }
}
