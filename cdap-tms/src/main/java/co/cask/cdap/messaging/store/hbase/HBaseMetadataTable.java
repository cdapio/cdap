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
import co.cask.cdap.data2.util.hbase.HTableDescriptorBuilder;
import co.cask.cdap.messaging.store.MetadataTable;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.TopicId;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * HBase implementation of {@link MetadataTable}.
 */
public class HBaseMetadataTable implements MetadataTable {
  private static final byte[] COL_FAMILY = Bytes.toBytes("cf");
  private static final byte[] COL = Bytes.toBytes("c");
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private final Configuration conf;
  private final HBaseTableUtil tableUtil;
  private final String tableName;
  private HBaseAdmin hBaseAdmin;
  private HTable hTable;

  public HBaseMetadataTable(Configuration conf, HBaseTableUtil tableUtil, String tableName) {
    this.conf = conf;
    this.tableUtil = tableUtil;
    this.tableName = tableName;
  }

  @Override
  public void createTableIfNotExists() throws IOException {
    TableId tableId = tableUtil.createHTableId(NamespaceId.CDAP, tableName);
    HColumnDescriptor hcd = new HColumnDescriptor(COL_FAMILY);
    HTableDescriptorBuilder htd = tableUtil.buildHTableDescriptor(tableId).addFamily(hcd);
    hBaseAdmin = new HBaseAdmin(conf);
    hTable = tableUtil.createHTable(conf, tableId);
    tableUtil.createTableIfNotExists(hBaseAdmin, tableId, htd.build());
  }

  @Nullable
  @Override
  public Map<String, String> getProperties(TopicId topicId) throws IOException {
    Get get = tableUtil.buildGet(getKey(topicId))
      .addFamily(COL_FAMILY)
      .build();
    Result result = hTable.get(get);
    byte[] value = result.getValue(COL_FAMILY, COL);
    if (value == null) {
      return null;
    }
    return GSON.fromJson(Bytes.toString(value), MAP_TYPE);
  }

  @Override
  public void createTopic(TopicId topicId, @Nullable Map<String, String> properties) throws IOException {
    if (properties == null) {
      properties = new HashMap<>();
    }

    Put put = tableUtil.buildPut(getKey(topicId))
      .add(COL_FAMILY, COL, Bytes.toBytes(GSON.toJson(properties)))
      .build();
    hTable.put(put);
  }

  @Override
  public void deleteTopic(TopicId topicId) throws IOException {
    Delete delete = tableUtil.buildDelete(getKey(topicId))
      .build();
    hTable.delete(delete);
  }

  @Override
  public List<TopicId> listTopics(NamespaceId namespaceId) throws IOException {
    List<TopicId> topicIds = new ArrayList<>();
    Scan scan = tableUtil.buildScan()
      .setStartRow(startKey(namespaceId))
      .addFamily(COL_FAMILY).build();

    try (ResultScanner resultScanner = hTable.getScanner(scan)) {
      Iterator<Result> resultIterator = resultScanner.iterator();
      while (resultIterator.hasNext()) {
        Result result = resultIterator.next();
        topicIds.add(getTopicId(result.getRow()));
      }
    }

    return topicIds;
  }

  @Override
  public void close() throws IOException {
    if (hTable != null) {
      hTable.close();
    }

    if (hBaseAdmin != null) {
      hBaseAdmin.close();
    }
  }

  private byte[] startKey(NamespaceId namespaceId) {
    return Bytes.toBytes(namespaceId.getNamespace() + ":");
  }

  private byte[] getKey(TopicId topicId) {
    Iterable<String> keyParts = topicId.toIdParts();
    String key = Joiner.on(":").join(keyParts);
    return Bytes.toBytes(key);
  }

  private TopicId getTopicId(byte[] key) {
    Iterable<String> keyParts = Splitter.on(":").split(Bytes.toString(key));
    return TopicId.fromIdParts(keyParts);
  }
}
