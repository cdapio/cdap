/*
 * Copyright © 2016-2017 Cask Data, Inc.
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
import co.cask.cdap.api.messaging.TopicAlreadyExistsException;
import co.cask.cdap.api.messaging.TopicNotFoundException;
import co.cask.cdap.data2.util.hbase.HBaseTableUtil;
import co.cask.cdap.data2.util.hbase.PutBuilder;
import co.cask.cdap.data2.util.hbase.ScanBuilder;
import co.cask.cdap.messaging.MessagingUtils;
import co.cask.cdap.messaging.TopicMetadata;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
public final class HBaseMetadataTable implements MetadataTable {

  public static final byte[] COL = MessagingUtils.Constants.METADATA_COLUMN;
  private static final Gson GSON = new Gson();

  // It has to be a sorted map since we depends on the serialized map for compareAndPut operation for topic update.
  private static final Type MAP_TYPE = new TypeToken<SortedMap<String, String>>() { }.getType();

  private final HBaseTableUtil tableUtil;
  private final byte[] columnFamily;
  private final HTable hTable;
  private final int scanCacheRows;
  private final HBaseExceptionHandler exceptionHandler;

  HBaseMetadataTable(HBaseTableUtil tableUtil, HTable hTable, byte[] columnFamily,
                     int scanCacheRows, HBaseExceptionHandler exceptionHandler) {
    this.tableUtil = tableUtil;
    this.hTable = hTable;
    this.columnFamily = Arrays.copyOf(columnFamily, columnFamily.length);
    this.scanCacheRows = scanCacheRows;
    this.exceptionHandler = exceptionHandler;
  }

  @Override
  public TopicMetadata getMetadata(TopicId topicId) throws IOException, TopicNotFoundException {
    Get get = tableUtil.buildGet(MessagingUtils.toMetadataRowKey(topicId))
      .addFamily(columnFamily)
      .build();

    try {
      Result result = hTable.get(get);
      byte[] value = result.getValue(columnFamily, COL);
      if (value == null) {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }

      Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
      TopicMetadata topicMetadata = new TopicMetadata(topicId, properties);
      if (!topicMetadata.exists()) {
        throw new TopicNotFoundException(topicId.getNamespace(), topicId.getTopic());
      }
      return topicMetadata;
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public void createTopic(TopicMetadata topicMetadata) throws TopicAlreadyExistsException, IOException {
    TopicId topicId = topicMetadata.getTopicId();

    byte[] rowKey = MessagingUtils.toMetadataRowKey(topicId);
    PutBuilder putBuilder = tableUtil.buildPut(rowKey);

    Get get = tableUtil.buildGet(rowKey)
      .addFamily(columnFamily)
      .build();

    try {
      boolean completed = false;
      while (!completed) {
        Result result = hTable.get(get);
        byte[] value = result.getValue(columnFamily, COL);

        if (value == null) {
          TreeMap<String, String> properties = new TreeMap<>(topicMetadata.getProperties());
          properties.put(TopicMetadata.GENERATION_KEY, MessagingUtils.Constants.DEFAULT_GENERATION);
          putBuilder.add(columnFamily, COL, Bytes.toBytes(GSON.toJson(properties, MAP_TYPE)));
          completed = hTable.checkAndPut(rowKey, columnFamily, COL, null, putBuilder.build());
        } else {
          Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
          TopicMetadata metadata = new TopicMetadata(topicId, properties);
          if (metadata.exists()) {
            throw new TopicAlreadyExistsException(topicId.getNamespace(), topicId.getTopic());
          }

          int newGenerationId = (metadata.getGeneration() * -1) + 1;
          TreeMap<String, String> newProperties = new TreeMap<>(properties);
          newProperties.put(TopicMetadata.GENERATION_KEY, Integer.toString(newGenerationId));

          putBuilder.add(columnFamily, COL, Bytes.toBytes(GSON.toJson(newProperties, MAP_TYPE)));
          completed = hTable.checkAndPut(rowKey, columnFamily, COL, value, putBuilder.build());
        }
      }
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public void updateTopic(TopicMetadata topicMetadata) throws TopicNotFoundException, IOException {
    byte[] rowKey = MessagingUtils.toMetadataRowKey(topicMetadata.getTopicId());
    boolean completed = false;

    try {
      // Keep trying to update
      while (!completed) {
        TopicMetadata oldMetadata = getMetadata(topicMetadata.getTopicId());
        TreeMap<String, String> newProperties = new TreeMap<>(topicMetadata.getProperties());
        newProperties.put(TopicMetadata.GENERATION_KEY, Integer.toString(oldMetadata.getGeneration()));

        Put put = tableUtil.buildPut(rowKey)
          .add(columnFamily, COL, Bytes.toBytes(GSON.toJson(newProperties, MAP_TYPE)))
          .build();
        byte[] oldValue = Bytes.toBytes(GSON.toJson(new TreeMap<>(oldMetadata.getProperties()), MAP_TYPE));
        completed = hTable.checkAndPut(rowKey, columnFamily, COL, oldValue, put);
      }
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public void deleteTopic(TopicId topicId) throws TopicNotFoundException, IOException {
    byte[] rowKey = MessagingUtils.toMetadataRowKey(topicId);
    boolean completed = false;

    try {
      // Keep trying to delete
      while (!completed) {
        TopicMetadata oldMetadata = getMetadata(topicId);
        TreeMap<String, String> newProperties = new TreeMap<>(oldMetadata.getProperties());
        newProperties.put(TopicMetadata.GENERATION_KEY, Integer.toString(oldMetadata.getGeneration() * -1));
        Put put = tableUtil.buildPut(rowKey)
          .add(columnFamily, COL, Bytes.toBytes(GSON.toJson(newProperties, MAP_TYPE)))
          .build();
        byte[] oldValue = Bytes.toBytes(GSON.toJson(new TreeMap<>(oldMetadata.getProperties()), MAP_TYPE));
        completed = hTable.checkAndPut(rowKey, columnFamily, COL, oldValue, put);
      }
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    byte[] startRow = MessagingUtils.topicScanKey(namespaceId);
    ScanBuilder scanBuilder = tableUtil.buildScan()
      .setStartRow(startRow)
      .setStopRow(Bytes.stopKeyForPrefix(startRow));
    return scanTopics(scanBuilder);
  }

  @Override
  public List<TopicId> listTopics() throws IOException {
    return scanTopics(tableUtil.buildScan());
  }

  /**
   * Scans the HBase table to get a list of {@link TopicId}.
   */
  private List<TopicId> scanTopics(ScanBuilder scanBuilder) throws IOException {
    Scan scan = scanBuilder.setFilter(new FirstKeyOnlyFilter()).setCaching(scanCacheRows).build();

    try {
      List<TopicId> topicIds = new ArrayList<>();
      try (ResultScanner resultScanner = hTable.getScanner(scan)) {
        for (Result result : resultScanner) {
          TopicId topicId = MessagingUtils.toTopicId(result.getRow());
          byte[] value = result.getValue(columnFamily, COL);
          Map<String, String> properties = GSON.fromJson(Bytes.toString(value), MAP_TYPE);
          TopicMetadata metadata = new TopicMetadata(topicId, properties);
          if (metadata.exists()) {
            topicIds.add(topicId);
          }
        }
      }
      return topicIds;
    } catch (IOException e) {
      throw exceptionHandler.handle(e);
    }
  }

  @Override
  public synchronized void close() throws IOException {
    hTable.close();
  }
}
