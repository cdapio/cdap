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
import co.cask.cdap.templates.etl.api.PipelineConfigurer;
import co.cask.cdap.templates.etl.api.Property;
import co.cask.cdap.templates.etl.api.StageConfigurer;
import co.cask.cdap.templates.etl.api.config.ETLStage;
import co.cask.cdap.templates.etl.api.realtime.RealtimeContext;
import co.cask.cdap.templates.etl.api.realtime.RealtimeSource;
import co.cask.cdap.templates.etl.api.realtime.SourceState;
import co.cask.cdap.templates.etl.realtime.kafka.Kafka08SimpleApiConsumer;
import co.cask.cdap.templates.etl.realtime.kafka.KafkaSimpleApiConsumer;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Map;

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

  public static final String KAFKA_PARTITIONS = "kafka.partitions";
  public static final String KAFKA_TOPIC = "kafka.topic";
  public static final String KAFKA_ZOOKEEPER = "kafka.zookeeper";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_DEFAULT_OFFSET = "kafka.default.offset";

  private static final String CDAP_KAFKA_SOURCE_NAME = "Kafka Realtime Source";

  private KafkaSimpleApiConsumer kafkaConsumer;

  @Override
  public void configure(StageConfigurer configurer) {
    configurer.setName(getClass().getSimpleName());
    configurer.setDescription(CDAP_KAFKA_SOURCE_NAME);
    configurer.addProperty(new Property(KAFKA_ZOOKEEPER, "The connect string location of Kafka Zookeeper", false));
    configurer.addProperty(new Property(KAFKA_BROKERS, "Comma separate list of Kafka brokers", false));
    configurer.addProperty(new Property(KAFKA_PARTITIONS, "Number of partitions" , true));
    configurer.addProperty(new Property(KAFKA_TOPIC, "Topic of the messages", true));
    configurer.addProperty(new Property(KAFKA_DEFAULT_OFFSET, "The default offset for the partition", false));
  }

  @Override
  public void configurePipeline(ETLStage stageConfig, PipelineConfigurer pipelineConfigurer) {
    Map<String, String> properties = stageConfig.getProperties();
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(KAFKA_PARTITIONS)),
                                "Number of partitions for Kafka topic need to specified.");
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(KAFKA_TOPIC)),
                                "The topic name of Kafka messages need to specified.");
    String zk = properties.get(KAFKA_ZOOKEEPER);
    String brokers = properties.get(KAFKA_BROKERS);
    Preconditions.checkArgument(!Strings.isNullOrEmpty(properties.get(zk)) || !Strings.isNullOrEmpty(brokers),
                                "Either Zookeeper connect info or list of brokers need to exists.");
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
