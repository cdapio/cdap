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

package co.cask.cdap.data2.metadata.publisher;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.twill.internal.kafka.client.ByteBufferEncoder;
import org.apache.twill.internal.kafka.client.IntegerEncoder;
import org.apache.twill.internal.kafka.client.IntegerPartitioner;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.Properties;

/**
 * A {@link MetadataChangePublisher} that publishes metadata changes to an Apache Kafka topic determined by the
 * configuration parameter <code>metadata.updates.kafka.topic</code>. External systems can get notifications of
 * metadata changes by subscribing to this topic.
 */
public class KafkaMetadataChangePublisher implements MetadataChangePublisher {
  private static final Logger LOG = LoggerFactory.getLogger(KafkaMetadataChangePublisher.class);
  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(Id.NamespacedId.class, new NamespacedIdCodec())
    .create();
  private final KafkaPublisher.Ack ack;
  private final Supplier<Producer<Integer, ByteBuffer>> producer;
  private final String topic;
  private final String brokerList;

  @Inject
  KafkaMetadataChangePublisher(CConfiguration cConf) {
    this.ack = KafkaPublisher.Ack.FIRE_AND_FORGET;
    this.topic = cConf.get(Constants.Metadata.UPDATES_KAFKA_TOPIC);
    this.brokerList = cConf.get(Constants.Metadata.UPDATES_KAFKA_BROKER_LIST);
    this.producer = Suppliers.memoize(new Supplier<Producer<Integer, ByteBuffer>>() {
      @Override
      public Producer<Integer, ByteBuffer> get() {
        Properties props = new Properties();
        props.put("metadata.broker.list", brokerList);
        props.put("serializer.class", ByteBufferEncoder.class.getName());
        props.put("key.serializer.class", IntegerEncoder.class.getName());
        props.put("partitioner.class", IntegerPartitioner.class.getName());
        props.put("request.required.acks", Integer.toString(ack.getAck()));
        props.put("compression.codec", Compression.SNAPPY.getCodec());
        ProducerConfig config = new ProducerConfig(props);
        return new Producer<>(config);
      }
    });
  }

  @Override
  public void publish(MetadataChangeRecord changeRecord) {
    byte[] changesToPublish = Bytes.toBytes(GSON.toJson(changeRecord));
    Object partitionKey = changeRecord.getPrevious().getTargetId();
    @SuppressWarnings("ConstantConditions")
    ByteBuffer message = ByteBuffer.wrap(changesToPublish);

    try {
      producer.get().send(new KeyedMessage<>(topic, Math.abs(partitionKey.hashCode()), message));
    } catch (Exception e) {
      LOG.error("Failed to send message to topic {} with broker list {}", topic, brokerList, e);
    }
  }
}
