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

package co.cask.cdap.templates.etl.realtime.sources;

import co.cask.cdap.templates.etl.api.Emitter;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.realtime.kafka.Kafka08SimpleApiConsumer;
import co.cask.cdap.templates.etl.realtime.kafka.KafkaSimpleApiConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import javax.annotation.Nullable;

/**
 * <p>
 *  Implementation of {@link RealtimeSource} that reads data from Kafka API and emit {@code ByteBuffer} as the
 *  output via {@link Emitter}.
 *
 *  This implementation have dependency on {@code Kafka} version 0.8.x.
 * </p>
 */
public class KafkaSource extends RealtimeSource<ByteBuffer> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  private static final String CDAP_KAFKA_SOURCE_NAME = "Kafka Realtime Source";

  private KafkaSimpleApiConsumer kafkaConsumer;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(CDAP_KAFKA_SOURCE_NAME);
    configurer.setDescription(CDAP_KAFKA_SOURCE_NAME);
    configurer.addProperty(new Property("kafka.zookeeper ", "The connect string location of Kafka Zookeeper", false));
    configurer.addProperty(new Property("kafka.brokers", "Comma separate list of Kafka brokers", false));
    configurer.addProperty(new Property("kafka.partitions", "Number of partitions" , true));
    configurer.addProperty(new Property("kafka.topic", "Topic of the messages", true));
    configurer.addProperty(new Property("kafka.default.offset", "The default offset for the partition", false));
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);

    kafkaConsumer = new Kafka08SimpleApiConsumer();
    kafkaConsumer.initialize(context);
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public SourceState poll(Emitter<ByteBuffer> writer, SourceState currentState) {
    // Lets set the internal offset store
    kafkaConsumer.saveState(currentState);

    kafkaConsumer.pollMessages(writer);

    // Update current state
    return new SourceState(kafkaConsumer.getSavedState());
  }
}
