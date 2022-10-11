/*
 * Copyright © 2016 Cask Data, Inc.
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

package io.cdap.cdap.messaging.store.leveldb;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import io.cdap.cdap.api.common.Bytes;
import io.cdap.cdap.api.dataset.lib.AbstractCloseableIterator;
import io.cdap.cdap.api.dataset.lib.CloseableIterator;
import io.cdap.cdap.api.messaging.TopicAlreadyExistsException;
import io.cdap.cdap.api.messaging.TopicNotFoundException;
import io.cdap.cdap.messaging.MessagingUtils;
import io.cdap.cdap.messaging.TopicMetadata;
import io.cdap.cdap.messaging.data.TopicId;
import io.cdap.cdap.messaging.store.MetadataTable;
import io.cdap.cdap.proto.id.NamespaceId;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MetadataTable}.
 */
final class LevelDBMetadataTable implements MetadataTable {

  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);

  private final DB levelDB;

  LevelDBMetadataTable(DB levelDB) {
    this.levelDB = levelDB;
  }

  DB getLevelDB() {
    return levelDB;
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    try {
      byte[] value = levelDB.get(MessagingUtils.toMetadataRowKey(new io.cdap.cdap.proto.id.TopicId(topicId)));
      if (value == null) {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }

      Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
      TopicMetadata topicMetadata = new TopicMetadata(topicId, properties);
      if (!topicMetadata.exists()) {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }
      return topicMetadata;
    } catch (DBException e) {
      // DBException is a RuntimeException. Turn it to IOException so that it forces caller to handle it.
      throw new IOException(e);
    }
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    try {
      TopicId topicId = topicMetadata.getTopicId();
      byte[] key = MessagingUtils.toMetadataRowKey(new io.cdap.cdap.proto.id.TopicId(topicId));
      TreeMap<String, String> properties = new TreeMap<>(topicMetadata.getProperties());
      properties.put(TopicMetadata.GENERATION_KEY, MessagingUtils.Constants.DEFAULT_GENERATION);

      synchronized (this) {
        byte[] tableValue = levelDB.get(key);
        if (tableValue != null) {
          Map<String, String> oldProperties = GSON.fromJson(Bytes.toString(tableValue), MAP_TYPE);
          TopicMetadata metadata = new TopicMetadata(topicId, oldProperties);
          if (metadata.exists()) {
            throw new TopicAlreadyExistsException(topicId.getNamespace(), topicId.getTopic());
          }

          int newGenerationId = (metadata.getGeneration() * -1) + 1;
          properties.put(TopicMetadata.GENERATION_KEY, Integer.toString(newGenerationId));
        }
        byte[] value = Bytes.toBytes(GSON.toJson(properties, MAP_TYPE));
        levelDB.put(key, value, WRITE_OPTIONS);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    try {
      TopicId topicId = topicMetadata.getTopicId();
      byte[] key = MessagingUtils.toMetadataRowKey(new io.cdap.cdap.proto.id.TopicId(topicId));
      synchronized (this) {
        byte[] tableValue = levelDB.get(key);
        if (tableValue == null) {
          throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
        }

        Map<String, String> oldProperties = GSON.fromJson(Bytes.toString(tableValue), MAP_TYPE);
        TopicMetadata oldMetadata = new TopicMetadata(topicId, oldProperties);
        if (!oldMetadata.exists()) {
          throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
        }

        TreeMap<String, String> newProperties = new TreeMap<>(topicMetadata.getProperties());
        newProperties.put(TopicMetadata.GENERATION_KEY, Integer.toString(oldMetadata.getGeneration()));
        levelDB.put(key, Bytes.toBytes(GSON.toJson(newProperties, MAP_TYPE)), WRITE_OPTIONS);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    byte[] rowKey = MessagingUtils.toMetadataRowKey(new io.cdap.cdap.proto.id.TopicId(topicId));
    try {
      synchronized (this) {
        byte[] tableValue = levelDB.get(rowKey);
        if (tableValue == null) {
          throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
        }

        Map<String, String> oldProperties = GSON.fromJson(Bytes.toString(tableValue), MAP_TYPE);
        TopicMetadata metadata = new TopicMetadata(topicId, oldProperties);
        if (!metadata.exists()) {
          throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
        }

        // Mark the topic as deleted
        TreeMap<String, String> newProperties = new TreeMap<>(metadata.getProperties());
        newProperties.put(TopicMetadata.GENERATION_KEY, Integer.toString(-1 * metadata.getGeneration()));
        levelDB.put(rowKey, Bytes.toBytes(GSON.toJson(newProperties, MAP_TYPE)), WRITE_OPTIONS);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<TopicId> listTopics(String namespaceId) throws IOException {
    byte[] startKey = MessagingUtils.topicScanKey(new NamespaceId(namespaceId));
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    return listTopics(startKey, stopKey);
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    return listTopics(null, null);
  }

  /**
   * Returns an iterator of {@link TopicMetadata} of all the topics including the ones that were deleted.
   *
   * @return {@link CloseableIterator} of {@link TopicMetadata}
   * @throws IOException if failed to scan topics
   */
  public CloseableIterator<TopicMetadata> scanTopics() throws IOException {
    return scanTopics(null, null);
  }

  private List<TopicId> listTopics(@Nullable byte[] startKey, @Nullable byte[] stopKey) throws IOException {
    List<TopicId> topicList = new ArrayList<>();
    try (CloseableIterator<TopicMetadata> iterator = scanTopics(startKey, stopKey)) {
      while (iterator.hasNext()) {
        TopicMetadata metadata = iterator.next();
        if (metadata.exists()) {
          topicList.add(metadata.getTopicId());
        }
      }
    }
    return topicList;
  }

  private CloseableIterator<TopicMetadata> scanTopics(@Nullable byte[] startKey,
                                                     @Nullable byte[] stopKey) throws IOException {
    final CloseableIterator<Map.Entry<byte[], byte[]>> iterator = new DBScanIterator(levelDB, startKey, stopKey);
    return new AbstractCloseableIterator<TopicMetadata>() {
      private boolean closed;

      @Override
      protected TopicMetadata computeNext() {
        if (closed || (!iterator.hasNext())) {
          return endOfData();
        }

        Map.Entry<byte[], byte[]> entry = iterator.next();
        TopicId topicId = MessagingUtils.toTopicId(entry.getKey()).toSpiTopicId();
        Map<String, String> properties = GSON.fromJson(Bytes.toString(entry.getValue()), MAP_TYPE);
        return new TopicMetadata(topicId, properties);
      }

      @Override
      public void close() {
        try {
          iterator.close();
        } finally {
          endOfData();
          closed = true;
        }
      }
    };
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}
