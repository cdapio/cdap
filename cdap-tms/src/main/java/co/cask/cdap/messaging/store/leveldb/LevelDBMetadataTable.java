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
import co.cask.cdap.api.dataset.table.Row;
import co.cask.cdap.api.dataset.table.Scanner;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

/**
 * LevelDB implementation of {@link MetadataTable}.
 */
public class LevelDBMetadataTable implements MetadataTable {

  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final LevelDBTableCore core;

  public LevelDBMetadataTable(LevelDBTableService service, String tableName) throws IOException {
    this.core = new LevelDBTableCore(tableName, service);
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    NavigableMap<byte[], byte[]> row = core.getRow(Bytes.toBytes(topicId.getNamespace()),
                                                   Bytes.toByteArrays(topicId.getTopic()), null, null, -1, null);
    if (row.isEmpty()) {
      throw new TopicNotFoundException(topicId);
    }

    Map<String, String> properties = GSON.fromJson(Bytes.toString(row.firstEntry().getValue()), MAP_TYPE);
    return new TopicMetadata(topicId, properties);
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws IOException {
    byte[] value = Bytes.toBytes(GSON.toJson(topicMetadata.getProperties()));

    TopicId topicId = topicMetadata.getTopicId();
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
    for (byte[] topic : topics.keySet()) {
      topicIds.add(new TopicId(namespaceId.getNamespace(), Bytes.toString(topic)));
    }
    return topicIds;
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    List<TopicId> topicIds = new ArrayList<>();
    try (Scanner scanner = core.scan(null, null, null, null, null)) {
      Row row = scanner.next();
      while (row != null) {
        for (byte[] topic : row.getColumns().keySet()) {
          topicIds.add(new TopicId(Bytes.toString(row.getRow()), Bytes.toString(topic)));
        }
        row = scanner.next();
      }
    }
    return topicIds;
  }

  @Override
  public void close() throws IOException {
    // no op
  }
}
