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

package co.cask.cdap.metadata;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.codec.NamespacedIdCodec;
import co.cask.cdap.proto.metadata.MetadataChangeRecord;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.inject.Inject;
import org.apache.twill.kafka.client.Compression;
import org.apache.twill.kafka.client.KafkaClient;
import org.apache.twill.kafka.client.KafkaPublisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutionException;

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
  private static final String PUBLISHER_CACHE_KEY = "kafkaPublisher";
  private final KafkaPublisher.Ack ack;
  // This cache will always only contain a single entry, since we will always use the same key.
  // However, using it will take care of synchronization issues while creating a publisher.
  private final LoadingCache<String, KafkaPublisher> publishers;
  private final String topic;

  @Inject
  KafkaMetadataChangePublisher(CConfiguration cConf, final KafkaClient kafkaClient) {
    this.ack = KafkaPublisher.Ack.FIRE_AND_FORGET;
    this.topic = cConf.get(Constants.Metadata.METADATA_UPDATES_KAFKA_TOPIC);
    this.publishers = CacheBuilder.newBuilder().build(new CacheLoader<String, KafkaPublisher>() {
      @Override
      @SuppressWarnings("NullableProblems")
      public KafkaPublisher load(String key) throws Exception {
        return kafkaClient.getPublisher(ack, Compression.SNAPPY);
      }
    });
  }

  @Override
  public void publish(MetadataChangeRecord changeRecord) {
    KafkaPublisher publisher;
    try {
      publisher = publishers.get(PUBLISHER_CACHE_KEY);
    } catch (ExecutionException e) {
      LOG.warn("Unable to get kafka publisher, will not be able to publish metadata.");
      return;
    }
    byte[] changesToPublish = Bytes.toBytes(GSON.toJson(changeRecord));
    KafkaPublisher.Preparer preparer = publisher.prepare(topic);
    preparer.add(ByteBuffer.wrap(changesToPublish), changeRecord.getPrevious().getTargetId().hashCode());
    preparer.send();
  }
}
