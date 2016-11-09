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
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import javax.annotation.Nullable;

/**
 * LevelDB implementation of {@link MetadataTable}.
 */
public class LevelDBMetadataTable implements MetadataTable {
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final String tableName;
  private final LevelDBTableService service;
  private final LevelDBTableCore core;

  public LevelDBMetadataTable(LevelDBTableService service, String tableName) throws IOException {
    this.service = service;
    this.tableName = tableName;
    this.core = new LevelDBTableCore(tableName, service);
  }

  @Override
  public void createTableIfNotExists() throws IOException {
    service.ensureTableExists(tableName);
  }

  @Override
  public Map<String, String> getProperties(TopicId topicId) throws IOException {
    NavigableMap<byte[], byte[]> row = core.getRow(Bytes.toBytes(topicId.getNamespace()),
                                                   Bytes.toByteArrays(topicId.getTopic()), null, null, -1, null);
    if (row.isEmpty()) {
      return null;
    }
    return GSON.fromJson(Bytes.toString(row.firstEntry().getValue()), MAP_TYPE);
  }

  @Override
  public void createTopic(TopicId topicId, @Nullable Map<String, String> properties) throws IOException {
    if (properties == null) {
      properties = new HashMap<>();
    }
    byte[] value = Bytes.toBytes(GSON.toJson(properties));
    core.put(Bytes.toBytes(topicId.getNamespace()), Bytes.toBytes(topicId.getTopic()), value, -1);
  }

  @Override
  public void deleteTopic(TopicId topicId) throws IOException {
    core.deleteColumn(Bytes.toBytes(topicId.getNamespace()), Bytes.toBytes(topicId.getTopic()));
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    NavigableMap<byte[], byte[]> topics = core.getRow(Bytes.toBytes(namespaceId.getNamespace()), null, null,
                                                      null, -1, null);
    List<TopicId> topicIds = new ArrayList<>();
    for (Map.Entry<byte[], byte[]> topic : topics.entrySet()) {
      topicIds.add(new TopicId(namespaceId.getNamespace(), Bytes.toString(topic.getKey())));
    }
    return topicIds;
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}
