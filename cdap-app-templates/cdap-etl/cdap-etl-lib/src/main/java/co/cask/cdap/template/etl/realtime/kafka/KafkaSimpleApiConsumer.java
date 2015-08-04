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

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.template.etl.api.Emitter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.SourceState;
import co.cask.cdap.template.etl.realtime.source.KafkaSource;
import com.google.common.base.Charsets;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Service;
import org.apache.twill.kafka.client.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.GenericArrayType;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * <p>
 *   Wrapper class for {@code Apache Kafka} consumer using the simple APIs to manage the last offset for a particular
 *   partition of a topic.
 *
 *   Most of the code are borrowed from the {@code cdap-kafka-pack} project and modified to be suit the stream source.
 * </p>
 *
 * @param <KEY> Type of message key
 * @param <PAYLOAD> Type of message value
 * @param <OFFSET> Type of offset object
 */
public abstract class KafkaSimpleApiConsumer<KEY, PAYLOAD, OFFSET> {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaSimpleApiConsumer.class);

  protected static final int SO_TIMEOUT = 5 * 1000;           // 5 seconds.

  protected final KafkaSource kafkaSource;

  private final Function<KafkaConsumerInfo<OFFSET>, OFFSET> consumerToOffset =
    new Function<KafkaConsumerInfo<OFFSET>, OFFSET>() {
      @Override
      public OFFSET apply(KafkaConsumerInfo<OFFSET> input) {
        return input.getReadOffset();
      }
    };

  private Function<ByteBuffer, KEY> keyDecoder;
  private Function<ByteBuffer, PAYLOAD> payloadDecoder;
  private KafkaConfig kafkaConfig;
  private Map<String, byte[]> offsetStore;

  private RealtimeContext sourceContext;
  private DefaultKafkaConfigurer kafkaConfigurer;

  private volatile Map<TopicPartition, KafkaConsumerInfo<OFFSET>> consumerInfos;

  protected KafkaSimpleApiConsumer(KafkaSource kafkaSource) {
    this.kafkaSource = kafkaSource;
  }

  /**
   * <p>
   *   The method should be called to initialized the consumer when being used by the {@code RealtimeSource}
   * </p>
   *
   * @param context
   * @throws Exception
   */
  public void initialize(RealtimeContext context) throws Exception {
    sourceContext = context;

    // Tries to detect key and payload decoder based on the class parameterized type.
    Type superType = TypeToken.of(getClass()).getSupertype(KafkaSimpleApiConsumer.class).getType();

    // Tries to detect Key and Payload type for creating decoder for them
    if (superType instanceof ParameterizedType) {
      // Extract Key and Payload types
      Type[] typeArgs = ((ParameterizedType) superType).getActualTypeArguments();

      keyDecoder = createKeyDecoder(typeArgs[0]);
      payloadDecoder = createPayloadDecoder(typeArgs[1]);
    }

    // Configure kafka
    kafkaConfigurer = new DefaultKafkaConfigurer();
    configureKafka(kafkaConfigurer);

    if (kafkaConfigurer.getZookeeper() == null && kafkaConfigurer.getBrokers() == null) {
      throw new IllegalStateException("Kafka not configured. Must provide either zookeeper or broker list.");
    }

    kafkaConfig = new KafkaConfig(kafkaConfigurer.getZookeeper(), kafkaConfigurer.getBrokers());
  }

  /**
   * Return the {@link RealtimeContext} for this Kafka source.
   *
   * @return instance of {@link RealtimeContext}
   */
  RealtimeContext getContext() {
    return sourceContext;
  }

  /**
   *
   * @return the name of this Kafka realtime source. Default would be the simple class name.
   */
  public String getName() {
    return getClass().getSimpleName();
  }

  /**
   * @return the internal state of topic partitions offset
   */
  public Map<String, byte[]> getSavedState() {
    return Maps.newHashMap(offsetStore);
  }

  /**
   *  Set the internal state for the Topic partition.
   */
  public void saveState(SourceState currentState) {
    // Update the internal state
    offsetStore = currentState.getState();
  }

  /**
   * Will be called by external source to start poll the Kafka messages one at the time.
   *
   * @param emitter instance of {@link Emitter} to emit the messages.
   */
  public void pollMessages(Emitter<StructuredRecord> emitter) {
    // Configure consumers late to read from Adapter SourceState
    if (consumerInfos == null) {
        consumerInfos = createConsumerInfos(kafkaConfigurer.getTopicPartitions());
    }

    boolean infosUpdated = false;
    // Poll for messages from Kafka
    for (KafkaConsumerInfo<OFFSET> info : consumerInfos.values()) {
      Iterator<KafkaMessage<OFFSET>> iterator = readMessages(info);
      while (iterator.hasNext()) {
        KafkaMessage<OFFSET> message = iterator.next();
        processMessage(message, emitter);

        // Update the read offset
        info.setReadOffset(message.getNextOffset());
      }
      if (info.hasPendingChanges()) {
        infosUpdated = true;
      }
    }

    // Save new offset if there is at least one message processed, or even if the offset simply changed.
    if (infosUpdated) {
      saveReadOffsets(Maps.transformValues(consumerInfos, consumerToOffset));
    }
  }

  /**
   * Configure Kafka consumer. This method will be called during the initialize phase
   *
   * @param configurer for configuring consuming from Kafka
   */
  protected abstract void configureKafka(KafkaConfigurer configurer);

  /**
   * Read messages from Kafka.
   *
   * @param consumerInfo Contains information about where to fetch messages from
   * @return An {@link Iterator} containing sequence of messages read from Kafka. The first message must
   *         has offset no earlier than the {@link KafkaConsumerInfo#getReadOffset()} as given in the parameter.
   */
  protected abstract Iterator<KafkaMessage<OFFSET>> readMessages(KafkaConsumerInfo<OFFSET> consumerInfo);

  /**
   * Returns the read offsets to start with for the given {@link TopicPartition}.
   */
  protected abstract OFFSET getBeginOffset(TopicPartition topicPartition);

  /**
   * Persists read offsets for all topic-partition that this Flowlet consumes from Kafka.
   */
  protected abstract void saveReadOffsets(Map<TopicPartition, OFFSET> offsets);

  /**
   * Should be called for clean up.
   */
  public void destroy() {
    // no-op
  }

  /**
   * Returns a Kafka configuration.
   */
  protected final KafkaConfig getKafkaConfig() {
    return kafkaConfig;
  }

  /**
   * Overrides this method if interested in the raw Kafka message.
   *
   * @param message The message fetched from Kafka.
   * @param emitter The emitter to push the message
   */
  protected void processMessage(KafkaMessage<OFFSET> message, Emitter<StructuredRecord> emitter) {
    // Should only receive messages from partitions that it can process
    int partition = message.getTopicPartition().getPartition();
    Preconditions.checkArgument((partition % getContext().getInstanceCount()) == getContext().getInstanceId(),
                                "Received unexpected partition " + partition);

    if (message.getKey() == null) {
      processMessage(decodePayload(message.getPayload()), emitter);
    } else {
      processMessage(decodeKey(message.getKey()), decodePayload(message.getPayload()), emitter);
    }
  }

  /**
   * Override this method if interested in both the key and payload of a message read from Kafka.
   *
   * @param key Key decoded from the message
   * @param payload Payload decoded from the message
   * @param emitter The emitter to push the message
   */
  protected void processMessage(KEY key, PAYLOAD payload, Emitter<StructuredRecord> emitter) {
    processMessage(payload, emitter);
  }

  /**
   * Override this method if only interested in the payload of a message read from Kafka.
   *
   * @param payload Payload decoded from the message
   * @param emitter The emitter to push the message
   */
  protected abstract void processMessage(PAYLOAD payload, Emitter<StructuredRecord> emitter);

  /**
   * Returns the default value of offset to start with when encounter a new broker for a given topic partition.
   * <p/>
   * By default, it is {@link kafka.api.OffsetRequest#EarliestTime()}. Sub-classes can override this to return
   * different value (for example {@link kafka.api.OffsetRequest#LatestTime()}).
   */
  protected abstract long getDefaultOffset(TopicPartition topicPartition);

  /**
   * Override this method to provide custom decoding of a message key.
   *
   * @param buffer The bytes representing the key in the Kafka message
   * @return The decoded key
   */
  @Nullable
  protected KEY decodeKey(ByteBuffer buffer) {
    return (keyDecoder != null) ? keyDecoder.apply(buffer) : null;
  }

  /**
   * Override this method to provide custom decoding of a message payload.
   *
   * @param buffer The bytes representing the payload in the Kafka message
   * @return The decoded payload
   */
  @Nullable
  protected PAYLOAD decodePayload(ByteBuffer buffer) {
    return (payloadDecoder != null) ? payloadDecoder.apply(buffer) : null;
  }

  /**
   * Stops a {@link Service} and waits for the completion. If there is exception during stop, it will get logged.
   */
  protected final void stopService(Service service) {
    try {
      service.stopAndWait();
    } catch (Throwable t) {
      LOG.error("Failed when stopping service {}", service, t);
    }
  }

  /**
   * Returns the key to be used when persisting offsets into the {@link SourceState}.
   */
  protected String getStoreKey(TopicPartition topicPartition) {
    return topicPartition.getTopic() + ":" + topicPartition.getPartition();
  }

  protected Map<String, byte[]> getOffsetStore() {
    return offsetStore;
  }

  protected StructuredRecord byteBufferToStructuredRecord(@Nullable String key, ByteBuffer payload) {
    return kafkaSource.byteBufferToStructuredRecord(key, payload);
  }

  /**
   * Creates a decoder for the key type.
   *
   * @param type type to decode to
   */
  private Function<ByteBuffer, KEY> createKeyDecoder(Type type) {
    return createDecoder(type, "No decoder for decoding message key");
  }

  /**
   * Creates a decoder for the payload type.
   *
   * @param type type to decode to
   */
  private Function<ByteBuffer, PAYLOAD> createPayloadDecoder(Type type) {
    return createDecoder(type, "No decoder for decoding message payload");
  }

  /**
   * Creates a decoder for decoding {@link ByteBuffer} for known type. It supports
   * <p/>
   * <pre>
   * - String (assuming UTF-8)
   * - byte[]
   * - ByteBuffer
   * </pre>
   *
   * @param type type to decode to
   * @param failureDecodeMessage message for the exception if decoding of the given type is not supported
   * @param <T> Type of the decoded type
   * @return A {@link Function} that decode {@link ByteBuffer} into the given type or a failure decoder created through
   *         {@link #createFailureDecoder(String)} if the type is not support
   */
  @SuppressWarnings("unchecked")
  private <T> Function<ByteBuffer, T> createDecoder(Type type, String failureDecodeMessage) {
    if (String.class.equals(type)) {
      return (Function<ByteBuffer, T>) createStringDecoder();
    }
    if (ByteBuffer.class.equals(type)) {
      return (Function<ByteBuffer, T>) createByteBufferDecoder();
    }
    if (byte[].class.equals(type) || (type instanceof GenericArrayType &&
      byte.class.equals(((GenericArrayType) type).getGenericComponentType()))) {
      return (Function<ByteBuffer, T>) createBytesDecoder();
    }
    return createFailureDecoder(failureDecodeMessage);
  }

  /**
   * Creates a decoder that convert the input {@link ByteBuffer} into UTF-8 String. The input {@link ByteBuffer}
   * will not be consumed after the call.
   */
  private Function<ByteBuffer, String> createStringDecoder() {
    return new Function<ByteBuffer, String>() {
      @Override
      public String apply(ByteBuffer input) {
        input.mark();
        String result = Bytes.toString(input, Charsets.UTF_8);
        input.reset();
        return result;
      }
    };
  }

  /**
   * Creates a decoder that returns the same input {@link ByteBuffer}.
   */
  private Function<ByteBuffer, ByteBuffer> createByteBufferDecoder() {
    return new Function<ByteBuffer, ByteBuffer>() {
      @Override
      public ByteBuffer apply(ByteBuffer input) {
        return input;
      }
    };
  }

  /**
   * Creates a decoder that reads {@link ByteBuffer} content and return it as {@code byte[]}. The
   * input {@link ByteBuffer} will not be consumed after the call.
   */
  private Function<ByteBuffer, byte[]> createBytesDecoder() {
    return new Function<ByteBuffer, byte[]>() {
      @Override
      public byte[] apply(ByteBuffer input) {
        byte[] bytes = new byte[input.remaining()];
        input.mark();
        input.get(bytes);
        input.reset();
        return bytes;
      }
    };
  }

  /**
   * Creates a decoder that always decode fail by raising an {@link IllegalStateException}.
   */
  private <T> Function<ByteBuffer, T> createFailureDecoder(final String failureMessage) {
    return new Function<ByteBuffer, T>() {
      @Override
      public T apply(ByteBuffer input) {
        throw new IllegalStateException(failureMessage);
      }
    };
  }

  /**
   * Helper method to create new {@link KafkaConsumerInfo} for map of topic partitions.
   *
   * @param config
   * @return KafkaConsumerInfo mapped to TopicPartitions
   */
  private Map<TopicPartition, KafkaConsumerInfo<OFFSET>> createConsumerInfos(Map<TopicPartition, Integer> config) {
    ImmutableMap.Builder<TopicPartition, KafkaConsumerInfo<OFFSET>> consumers = ImmutableMap.builder();

    for (Map.Entry<TopicPartition, Integer> entry : config.entrySet()) {
      consumers.put(entry.getKey(),
                    new KafkaConsumerInfo<>(entry.getKey(), entry.getValue(), getBeginOffset(entry.getKey())));
    }
    return consumers.build();
  }

}
