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
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.DBException;
import org.iq80.leveldb.DBIterator;
import org.iq80.leveldb.WriteOptions;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MetadataTable}.
 */
public class LevelDBMetadataTable implements MetadataTable {

  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static final WriteOptions WRITE_OPTIONS = new WriteOptions().sync(true);

  private final DB levelDB;

  public LevelDBMetadataTable(DB levelDB) throws IOException {
    this.levelDB = levelDB;
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    try {
      byte[] value = levelDB.get(getKey(topicId));
      if (value == null) {
        throw new TopicNotFoundException(topicId);
      }

      Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
      return new TopicMetadata(topicId, properties);
    } catch (DBException e) {
      // DBException is a RuntimeException. Turn it to IOException so that it forces caller to handle it.
      throw new IOException(e);
    }
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws IOException {
    try {
      TopicId topicId = topicMetadata.getTopicId();
      byte[] value = Bytes.toBytes(GSON.toJson(topicMetadata.getProperties()));
      levelDB.put(getKey(topicId), value, WRITE_OPTIONS);
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void deleteTopic(TopicId topicId) throws IOException {
    try {
      levelDB.delete(getKey(topicId));
    } catch (DBException e) {
      throw new IOException(e);
    }
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    byte[] startKey = startKey(namespaceId);
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
    try (DBIterator iterator = levelDB.iterator()) {
      iterator.seek(startKey == null ? Bytes.EMPTY_BYTE_ARRAY : startKey);
      while (iterator.hasNext()) {
        Map.Entry<byte[], byte[]> entry = iterator.next();
        // If passed the stopKey, which is the end of the current namespace, break the loop.
        if (stopKey != null && Bytes.compareTo(entry.getKey(), stopKey) >= 0) {
          break;
        }
        topicIds.add(getTopicId(entry.getKey()));
      }
    }
    return topicIds;

  }

  /**
   * Returns the start key for scanning topics under the given namespace.
   */
  private byte[] startKey(NamespaceId namespaceId) {
    return Bytes.toBytes(namespaceId.getNamespace() + ":");
  }

  /**
   * Returns the put key for the given topic.
   */
  private byte[] getKey(TopicId topicId) {
    return Bytes.toBytes(Joiner.on(':').join(topicId.toIdParts()));
  }

  /**
   * Decodes the given row key into {@link TopicId}.
   */
  private TopicId getTopicId(byte[] key) {
    return TopicId.fromIdParts(Splitter.on(":").split(Bytes.toString(key)));
  }
}
