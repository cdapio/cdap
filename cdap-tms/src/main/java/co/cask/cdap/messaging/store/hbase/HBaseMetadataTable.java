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

package co.cask.cdap.messaging.store.hbase;

import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicAlreadyExistsException;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FirstKeyOnlyFilter;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * HBase implementation of {@link MetadataTable}.
 */
final class HBaseMetadataTable implements MetadataTable {

  private static final byte[] COL = Bytes.toBytes("m");
  private static final Gson GSON = new Gson();
  // It has to be a sorted map since we depends on the serialized map for compareAndPut operation for topic update.
  private static final Type MAP_TYPE = new TypeToken<SortedMap<String, String>>() { }.getType();

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final HTable hTable;

  HBaseMetadataTable(HBaseTableUtil tableUtil, HTable hTable, byte[] columnFamily) {
    this.tableUtil = tableUtil;
    this.hTable = hTable;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    Get get = tableUtil.buildGet(MessagingUtils.toRowKeyPrefix(topicId))
      .addFamily(columnFamily)
      .build();

    Result result = hTable.get(get);
    byte[] value = result.getValue(columnFamily, COL);
    if (value == null) {
      throw new TopicNotFoundException(topicId);
    }

    Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
    return new TopicMetadata(topicId, properties);
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    byte[] rowKey = MessagingUtils.toRowKeyPrefix(topicMetadata.getTopicId());
    Put put = tableUtil.buildPut(rowKey)
      .add(columnFamily, COL, Bytes.toBytes(GSON.toJson(new TreeMap<>(topicMetadata.getProperties()), MAP_TYPE)))
      .build();

    if (!hTable.checkAndPut(rowKey, columnFamily, COL, null, put)) {
      throw new TopicAlreadyExistsException(topicMetadata.getTopicId());
    }
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    boolean completed = false;

    // Keep trying to update
    byte[] rowKey = MessagingUtils.toRowKeyPrefix(topicMetadata.getTopicId());
    Put put = tableUtil.buildPut(rowKey)
      .add(columnFamily, COL, Bytes.toBytes(GSON.toJson(new TreeMap<>(topicMetadata.getProperties()), MAP_TYPE)))
      .build();

    while (!completed) {
      TopicMetadata oldMetadata = getMetadata(topicMetadata.getTopicId());
      byte[] oldValue = Bytes.toBytes(GSON.toJson(new TreeMap<>(oldMetadata.getProperties()), MAP_TYPE));
      completed = hTable.checkAndPut(rowKey, columnFamily, COL, oldValue, put);
    }
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    boolean completed = false;

    // Keep trying to delete
    byte[] rowKey = MessagingUtils.toRowKeyPrefix(topicId);
    Delete delete = tableUtil.buildDelete(rowKey).build();
    while (!completed) {
      TopicMetadata metadata = getMetadata(topicId);
      byte[] oldValue = Bytes.toBytes(GSON.toJson(new TreeMap<>(metadata.getProperties()), MAP_TYPE));
      completed = hTable.checkAndDelete(rowKey, columnFamily, COL, oldValue, delete);
    }
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    byte[] startRow = MessagingUtils.topicScanKey(namespaceId);
    Scan scan = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(startRow))
      .build();
    return scanTopics(scan);
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    return scanTopics(tableUtil.buildScan().build());
  }

  /**
   * Scans the HBase table to get a list of {@link TopicId}.
   */
  private List<TopicId> scanTopics(Scan scan) throws IOException {
    scan.setFilter(new FirstKeyOnlyFilter())
        .setCaching(1000);

    List<TopicId> topicIds = new ArrayList<>();
    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
      for (Result result : resultScanner) {
        topicIds.add(MessagingUtils.toTopicId(result.getRow()));
      }
    }
    return topicIds;
  }

  @Override
  public synchronized void close() throws IOException {
    hTable.close();
  }
}
