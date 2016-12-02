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

package co.cask.cdap.messaging.store.leveldb;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.dataset.lib.CloseableIterator;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicAlreadyExistsException;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.messaging.store.TopicMetadataCodec;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MetadataTable}.
 */
final class LevelDBMetadataTable implements MetadataTable {

  private static final Gson GSON = new GsonBuilder()
    .registerTypeAdapter(TopicMetadata.class, new TopicMetadataCodec())
    .create();
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);

  private final DB levelDB;

  LevelDBMetadataTable(DB levelDB) throws IOException {
    this.levelDB = levelDB;
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    try {
      byte[] value = levelDB.get(MessagingUtils.toMetadataRowKey(topicId));
      if (value == null) {
        throw new TopicNotFoundException(topicId);
      }

      TopicMetadata topicMetadata = GSON.fromJson(Bytes.toString(value), TopicMetadata.class);
      if (topicMetadata.isDeleted()) {
        throw new TopicNotFoundException(topicId);
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
      byte[] key = MessagingUtils.toMetadataRowKey(topicMetadata.getTopicId());
      byte[] value = Bytes.toBytes(GSON.toJson(topicMetadata));
      synchronized (this) {
        byte[] tableValue = levelDB.get(key);
        if (tableValue != null) {
          TopicMetadata metadata = GSON.fromJson(Bytes.toString(tableValue), TopicMetadata.class);
          if (!metadata.isDeleted()) {
            throw new TopicAlreadyExistsException(topicMetadata.getTopicId());
          }

          int newGenerationId = (metadata.getGeneration() * -1) + 1;
          TopicMetadata newMetadata = new TopicMetadata(metadata.getTopicId(), metadata.getProperties(),
                                                        newGenerationId);
          value = Bytes.toBytes(GSON.toJson(newMetadata));
        }
        levelDB.put(key, value, WRITE_OPTIONS);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    try {
      byte[] key = MessagingUtils.toMetadataRowKey(topicMetadata.getTopicId());
      synchronized (this) {
        byte[] tableValue = levelDB.get(key);
        if (tableValue == null) {
          throw new TopicNotFoundException(topicMetadata.getTopicId());
        }

        TopicMetadata oldMetadata = GSON.fromJson(Bytes.toString(tableValue), TopicMetadata.class);
        if (oldMetadata.isDeleted()) {
          throw new TopicNotFoundException(oldMetadata.getTopicId());
        }
        TopicMetadata newMetadata = new TopicMetadata(topicMetadata.getTopicId(), topicMetadata.getProperties(),
                                                      oldMetadata.getGeneration());
        levelDB.put(key, Bytes.toBytes(GSON.toJson(newMetadata)), WRITE_OPTIONS);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    byte[] rowKey = MessagingUtils.toMetadataRowKey(topicId);
    try {
      synchronized (this) {
        byte[] tableValue = levelDB.get(rowKey);
        if (tableValue == null) {
          throw new TopicNotFoundException(topicId);
        }

        TopicMetadata metadata = GSON.fromJson(Bytes.toString(tableValue), TopicMetadata.class);
        if (metadata.isDeleted()) {
          throw new TopicNotFoundException(metadata.getTopicId());
        }

        // Mark the topic as deleted
        TopicMetadata newMetadata = new TopicMetadata(metadata.getTopicId(), metadata.getProperties(),
                                                      -1 * metadata.getGeneration());
        levelDB.put(rowKey, Bytes.toBytes(GSON.toJson(newMetadata)), WRITE_OPTIONS);
      }
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    byte[] startKey = MessagingUtils.topicScanKey(namespaceId);
    byte[] stopKey = Bytes.stopKeyForPrefix(startKey);
    return scanTopics(startKey, stopKey);
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    return scanTopics(null, null);
  }

  @Override
  public void close() throws IOException {
    // no op
  }

  private List<TopicId> scanTopics(@Nullable byte[] startKey, @Nullable byte[] stopKey) throws IOException {
    List<TopicId> topicIds = new ArrayList<>();
    try (CloseableIterator<Map.Entry<byte[], byte[]>> iterator = new DBScanIterator(levelDB, startKey, stopKey)) {
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        TopicMetadata metadata = GSON.fromJson(Bytes.toString(entry.getValue()), TopicMetadata.class);
        if (!metadata.isDeleted()) {
          topicIds.add(MessagingUtils.toTopicId(entry.getKey()));
        }
      }
    }
    return topicIds;
  }
}
