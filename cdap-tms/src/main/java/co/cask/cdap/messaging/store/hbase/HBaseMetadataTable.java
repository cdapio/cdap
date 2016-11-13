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
import co.cask.cdap.data2.util.TableId;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.TopicNotFoundException;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
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

/**
 * HBase implementation of {@link MetadataTable}.
 */
public class HBaseMetadataTable implements MetadataTable {

  private static final byte[] COL = Bytes.toBytes("m");
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final Configuration hConf;
  private final HBaseTableUtil tableUtil;
  private final TableId tableId;
  private final byte[] columnFamily;
  private volatile HTable hTable;

  public HBaseMetadataTable(Configuration hConf, HBaseTableUtil tableUtil, TableId tableId, byte[] columnFamily) {
    this.hConf = hConf;
    this.tableUtil = tableUtil;
    this.tableId = tableId;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    Get get = tableUtil.buildGet(getKey(topicId))
      .addFamily(columnFamily)
      .build();

    Result result = getHTable().get(get);
    byte[] value = result.getValue(columnFamily, COL);
    if (value == null) {
      throw new TopicNotFoundException(topicId);
    }

    Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
    return new TopicMetadata(topicId, properties);
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws IOException {
    Put put = tableUtil.buildPut(getKey(topicMetadata.getTopicId()))
      .add(columnFamily, COL, Bytes.toBytes(GSON.toJson(topicMetadata.getProperties(), MAP_TYPE)))
      .build();
    getHTable().put(put);
  }

  @Override
  public void deleteTopic(TopicId topicId) throws IOException {
    Delete delete = tableUtil.buildDelete(getKey(topicId)).build();
    getHTable().delete(delete);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    byte[] startRow = startKey(namespaceId);
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
    try (ResultScanner resultScanner = getHTable().getScanner(scan)) {
      for (Result result : resultScanner) {
        topicIds.add(getTopicId(result.getRow()));
      }
    }
    return topicIds;
  }

  @Override
  public synchronized void close() throws IOException {
    if (hTable != null) {
      hTable.close();
    }
  }

  private HTable getHTable() throws IOException {
    if (hTable != null) {
      return hTable;
    }

    synchronized (this) {
      if (hTable != null) {
        return hTable;
      }
      hTable = tableUtil.createHTable(hConf, tableId);
      // Use auto flush since we are not doing any batch type operation on the metadata table.
      // Auto-flush will make sure every Put call is executed right away.
      hTable.setAutoFlush(true, false);
      return hTable;
    }
  }

  private byte[] startKey(NamespaceId namespaceId) {
    return Bytes.toBytes(namespaceId.getNamespace() + ":");
  }

  private byte[] getKey(TopicId topicId) {
    return Bytes.toBytes(Joiner.on(":").join(topicId.toIdParts()));
  }

  private TopicId getTopicId(byte[] key) {
    return TopicId.fromIdParts(Splitter.on(":").split(Bytes.toString(key)));
  }
}
