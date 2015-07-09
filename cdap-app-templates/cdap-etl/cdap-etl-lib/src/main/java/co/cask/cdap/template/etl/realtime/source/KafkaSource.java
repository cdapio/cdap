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

package co.cask.cdap.template.etl.realtime.source;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.FormatSpecification;
import co.cask.cdap.api.data.format.RecordFormat;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.flow.flowlet.StreamEvent;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.format.RecordFormats;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.PipelineConfigurer;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSource;
import co.cask.cdap.template.etl.api.realtime.SourceState;
import co.cask.cdap.template.etl.realtime.kafka.Kafka08SimpleApiConsumer;
import co.cask.cdap.template.etl.realtime.kafka.KafkaSimpleApiConsumer;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
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
@Plugin(type = "source")
@Name("Kafka")
@Description("Kafka Realtime Source - Emits a record with two fields - 'key' (nullable string) and 'message' (bytes)")
public class KafkaSource extends RealtimeSource<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);

  public static final String MESSAGE = "message";
  public static final String KEY = "key";

  public static final String KAFKA_PARTITIONS = "kafka.partitions";
  public static final String KAFKA_TOPIC = "kafka.topic";
  public static final String KAFKA_ZOOKEEPER = "kafka.zookeeper";
  public static final String KAFKA_BROKERS = "kafka.brokers";
  public static final String KAFKA_DEFAULT_OFFSET = "kafka.default.offset";
  public static final String KAFKA_SCHEMA = "kafka.schema";
  public static final String KAFKA_FORMAT = "kafka.format";

  private static final String FORMAT_SETTING_PREFIX = "format.setting.";

  private static final Schema SCHEMA = Schema.recordOf("Kafka Message",
                                                       Schema.Field.of(MESSAGE, Schema.of(Schema.Type.BYTES)),
                                                       Schema.Field.of(KEY, Schema.nullableOf(
                                                         Schema.of(Schema.Type.STRING))));
  private KafkaSimpleApiConsumer kafkaConsumer;
  private KafkaPluginConfig config;

  /**
   * Default constructor. This will primarily will be used to test.
   * @param config
   */
  public KafkaSource (KafkaPluginConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    // check the schema if there is one
    if (!Strings.isNullOrEmpty(config.schema)) {
      config.parseSchema();
    }
  }

  @Override
  public void initialize(RealtimeContext context) throws Exception {
    super.initialize(context);

    kafkaConsumer = new Kafka08SimpleApiConsumer(this);
    kafkaConsumer.initialize(context);
    if (config.format != null) {
      FormatSpecification spec = config.getFormatSpec();
      RecordFormat<ByteBuffer, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);
      format.initialize(spec);
    }
  }

  @Nullable
  @Override
  @SuppressWarnings("unchecked")
  public SourceState poll(Emitter<StructuredRecord> writer, SourceState currentState) {
    try {
      // Lets set the internal offset store
      kafkaConsumer.saveState(currentState);

      kafkaConsumer.pollMessages(writer);
    } catch (Throwable t) {
      LOG.error("Error encountered during poll to get message for Kafka source.", t);
      return currentState;
    }

    // Update current state
    return new SourceState(kafkaConsumer.getSavedState());
  }

  /**
   * Convert {@code Apache Kafka} ByteBuffer from message into CDAP {@link StructuredRecord} instance.
   * @param key the String key of the Kafka message
   * @param payload the ByteBuffer of the Kafka message.
   * @return instance of {@link StructuredRecord} representing the message using the appropriate format.
   */
  public StructuredRecord byteBufferToStructuredRecord(@Nullable String key, ByteBuffer payload) {
    FormatSpecification spec = config.getFormatSpec();
    if (config.format == null) {
      return byteBufferToSchemalessByteRecord(key, payload);
    }
    try {
      RecordFormat<StreamEvent, StructuredRecord> format = RecordFormats.createInitializedFormat(spec);
      StreamEvent toStream = new StreamEvent(payload);
      return format.read(toStream);
    } catch (Exception e) {
      LOG.debug("Could not parse Kafka payload into schema. Using default structured record instead.");
      return byteBufferToSchemalessByteRecord(key, payload);
    }
  }

  private StructuredRecord byteBufferToSchemalessByteRecord(@Nullable String key, ByteBuffer payload) {
    StructuredRecord.Builder recordBuilder = StructuredRecord.builder(SCHEMA);
    if (key != null) {
      recordBuilder.set(KEY, key);
    }
    recordBuilder.set(MESSAGE, payload);
    return recordBuilder.build();
  }

  /**
   * Get the internal config instance.
   *
   * @return the internal {@link KafkaPluginConfig} for this Kafka realtime source.
   */
  @Nullable
  public KafkaPluginConfig getConfig() {
    return config;
  }

  /**
   * Helper class to provide {@link PluginConfig} for {@link KafkaSource}.
   */
  public static class KafkaPluginConfig extends PluginConfig {

    @Name(KAFKA_PARTITIONS)
    @Description("Number of partitions.")
    private final Integer partitions;

    @Name(KAFKA_TOPIC)
    @Description("Topic of the messages.")
    private final String topic;

    @Name(KAFKA_ZOOKEEPER)
    @Description("The connect string location of Zookeeper. Either this one or the list of brokers is required.")
    @Nullable
    private final String zkConnect;

    @Name(KAFKA_BROKERS)
    @Description("Comma separated list of Kafka brokers. Either this one or Zookeeper connect info is required.")
    @Nullable
    private final String kafkaBrokers;

    @Name(KAFKA_DEFAULT_OFFSET)
    @Description("The default offset for the partition. Default value is kafka.api.OffsetRequest.EarliestTime.")
    @Nullable
    private final Long defaultOffset;

    @Name(KAFKA_SCHEMA)
    @Description("Optional schema for the body of Kafka events. Schema is used " +
      "in conjunction with format to parse Kafka payloads. Some formats like the avro format require schema, " +
      "while others do not. The schema given is for the body of the Kafka event")
    @Nullable
    private final String schema;

    @Name(KAFKA_FORMAT)
    @Description("Optional format of the Kafka event. Any format supported by CDAP is also supported. " +
      "For example, a value of 'csv' will attempt to parse Kafka payloads as comma separated values. " +
      "If no format is given, Kafka message payloads will be treated as bytes, resulting in a two field schema: " +
      "'key' of type string (which is nullable) and 'payload' of type bytes.")
    @Nullable
    private final String format;

    public KafkaPluginConfig(String zkConnect, String brokers, Integer partitions, String topic,
                             Long defaultOffset, @Nullable String format, @Nullable String schema) {
      this.zkConnect = zkConnect;
      this.kafkaBrokers = brokers;
      this.partitions = partitions;
      this.topic = topic;
      this.defaultOffset = defaultOffset;
      this.schema = schema;
      this.format = format;
    }

    // Accessors

    public Integer getPartitions() {
      return partitions;
    }

    public String getTopic() {
      return topic;
    }

    @Nullable
    public String getZkConnect() {
      return zkConnect;
    }

    @Nullable
    public String getKafkaBrokers() {
      return kafkaBrokers;
    }

    @Nullable
    public Long getDefaultOffset() {
      return defaultOffset;
    }

    @Nullable
    private FormatSpecification getFormatSpec() {
      FormatSpecification formatSpec = null;
      if (!Strings.isNullOrEmpty(format)) {
        // try to parse the schema if there is one
        Schema schemaObj = parseSchema();

        if (getProperties() != null) {
          // strip format.settings. from any properties and use them in the format spec
          ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
          for (Map.Entry<String, String> entry : getProperties().getProperties().entrySet()) {
            if (entry.getKey().startsWith(FORMAT_SETTING_PREFIX)) {
              String key = entry.getKey();
              builder.put(key.substring(FORMAT_SETTING_PREFIX.length(), key.length()), entry.getValue());
            }
          }
          formatSpec = new FormatSpecification(format, schemaObj, builder.build());
        } else {
          formatSpec = new FormatSpecification(format, schemaObj, null);
        }
      }
      return formatSpec;
    }

    private Schema parseSchema() {
      try {
        return Strings.isNullOrEmpty(schema) ? null : Schema.parseJson(schema);
      } catch (IOException e) {
        throw new IllegalArgumentException("Invalid schema: " + e.getMessage());
      }
    }
  }
}
