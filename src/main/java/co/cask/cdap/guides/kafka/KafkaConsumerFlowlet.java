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

import co.cask.cdap.api.annotation.UseDataSet;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.flow.flowlet.OutputEmitter;
import co.cask.cdap.kafka.flow.Kafka08ConsumerFlowlet;
import co.cask.cdap.kafka.flow.KafkaConfigurer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Kafka Subscription Flowlet.
 */
public class KafkaConsumerFlowlet extends Kafka08ConsumerFlowlet<byte[], String> {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerFlowlet.class);

  @UseDataSet(Constants.OFFSET_TABLE_NAME)
  private KeyValueTable offsetStore;

  private OutputEmitter<String> emitter;

  @Override
  protected void configureKafka(KafkaConfigurer kafkaConfigurer) {
    Map<String, String> runtimeArgs = getContext().getRuntimeArguments();
    kafkaConfigurer.setZooKeeper(runtimeArgs.get("kafka.zookeeper"));
    kafkaConfigurer.addTopicPartition(runtimeArgs.get("kafka.topic"), 0);
  }

  @Override
  protected KeyValueTable getOffsetStore() {
    return offsetStore;
  }

  @Override
  protected void processMessage(String value) throws Exception {
    LOG.info("Message: {}", value);
    emitter.emit(value);
  }
}
